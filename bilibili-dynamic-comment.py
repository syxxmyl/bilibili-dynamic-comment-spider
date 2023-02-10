import asyncio
import time
import os
import sys
import yaml
import pymysql

from loguru import logger
from bilibili_api import user, comment

from mysqlTest import MyMysqlConnect

# logger
commentLog = logger.bind(user="comment")
commentLog.remove()
commentLog.add(
    sys.stdout,
    colorize=True,
    backtrace=True,
    diagnose=True,)

# config
try:
    with open('users.yaml', 'r', encoding='utf-8') as f:
        users = yaml.load(f, Loader=yaml.FullLoader)
except Exception as e:
    commentLog.error(f"读取配置文件失败,请检查配置文件格式是否正确: {e}")
    exit(1)

uids = []

for u in users['USERS']:
    uidsStr = u.get('uids', '')
    mysqlip = u.get('mysqlip', 'localhost')
    mysqlport = u.get('mysqlport', 3306)
    mysqluser = u.get('mysqluser', 'root')
    mysqlpassword = u.get('mysqlpassword', '123456')
    uids.extend(list(map(lambda x :int(x if x else 0), str(uidsStr).split(','))))

commentLog.info(f"uidList={uids}")
# desc.type=1       : req param :   oid=desc.dynamic_id             type=17         # 转发
# desc.type=2       : req param :   oid=desc.rid     #相簿id        type=11         # 带图片的动态
# desc.type=4       : req param :   oid=desc.dynamic_id             type=17         # 纯文字动态
# desc.type=8       : req param :   oid=desc.rid     #av号          type=1          # 视频动态
# desc.type=2048    : req param :   oid=desc.dynamic_id             type=17         # H5活动动态
def TransformDynamicTypeToCommentType(dynamicType = 0):      
    if dynamicType == 2:
        return comment.ResourceType.DYNAMIC_DRAW    #11
    elif dynamicType == 8:
        return comment.ResourceType.VIDEO   #1

    return comment.ResourceType.DYNAMIC #17

def TransformDynamicOidToCommentOid(oid=0, rid=0, type=0):
    if type == 2 or type == 8:
        return rid
    
    return oid


async def get_all_dynamics():
    for uid in uids:
        fileName = str(uid) + "\dynamics_type.txt"
        if not os.path.exists(str(uid) + '/'):
            os.mkdir(str(uid))
        # if not os.path.exists(fileName):
        #     os.system(r"touch {}".format(fileName))
        dynamicFile = open(fileName, "w", encoding='utf-8')
        
        dynamicCommentReplyTimeDict = {}
        dynamicTimeFile = open(str(uid) + '/dynamic_time.txt', "a+", encoding='utf-8')
        dynamicTimeFile.seek(0)
        lines = dynamicTimeFile.readlines()
        for line in lines:
            strArr = line.split(" ")
            dynamicid = strArr[0].split("=")[1]
            time = strArr[1].split("=")[1][:-1]
            dynamicCommentReplyTimeDict[dynamicid] = time
        dynamicTimeFile.seek(0)
        dynamicTimeFile.truncate(0)
        # print(dynamicCommentReplyTimeDict)

        dynamicTimeBackupFile = open(str(uid) + '/dynamic_time_backup.txt', "w", encoding='utf-8')
        for data in dynamicCommentReplyTimeDict:
            dynamicTimeBackupFile.write(f"dynamicid={data} time={dynamicCommentReplyTimeDict[data]}\n")
            dynamicTimeBackupFile.flush()
        dynamicTimeBackupFile.close()

        usr = user.User(uid)
        offset = 0

        while True:
            
            dynamics = await usr.get_dynamics(offset=offset, need_top=True)
            if dynamics['has_more'] != 1:
                break
            offset = dynamics['next_offset']

            for dynamicInfo in dynamics['cards']:
                await asyncio.sleep(1)
                dynamicID = dynamicInfo['desc']['dynamic_id']
                dynamicType = dynamicInfo['desc']['type']
                dynamicRid = dynamicInfo['desc']['rid']
                dynamicFile.write(f"dynamicID={dynamicID}, dynamicType={dynamicType}, dynamicRid={dynamicRid}\n\n")
                dynamicFile.flush()
                maxCommentReplyTime = 0
                maxCommentReplyTime = dynamicCommentReplyTimeDict.get(str(dynamicID)) or 0
                commentLog.info(f"dynamicID={dynamicID} maxCommentReplyTime={maxCommentReplyTime}")
                try:
                    maxCommentReplyTime = await writeDynamicComment(uid, dynamicID, dynamicRid, dynamicType, int(maxCommentReplyTime))
                    dynamicTimeFile.write(f"dynamicid={dynamicID} time={maxCommentReplyTime}\n")
                    dynamicTimeFile.flush()
                except Exception as e:
                    commentLog.error("writeDynamicComment " + str(dynamicID) + "failed, error=" + str(e))
                    continue
            await asyncio.sleep(1)

        dynamicTimeFile.close()
        dynamicFile.close()

async def writeDynamicComment(uid=0, dynamicOid=0, dynamicRid=0, dynamicType=0, maxCommentReplyTime=0):
    
    fileName = str(uid) + "\dynamic_comment_" + str(dynamicOid) + ".txt"
    commentLog.info(fileName)
    if not os.path.exists(str(uid) + '/'):
        os.mkdir(str(uid))
    dynamicFile = open(fileName, "a+", encoding='utf-8')

    myConnect = MyMysqlConnect(host=mysqlip, port=mysqlport, user=mysqluser, password=mysqlpassword)
    myConnect.CreateDatabase(database="UID" + str(uid))
    myConnect.UseDatabase(database="UID" + str(uid))
    myConnect.CreateCommentTable(tablename=str(dynamicOid))

    comments = []
    page = 1
    count = 0

    currentMaxCommentReplyTime = 0
    isFinish = False
    lastCount = 0
    insertCount = 0

    has_load_upper = False

    while True:
        
        if isFinish:
            break
        
        await asyncio.sleep(1)

        try:
            dynamicInfo = await comment.get_comments(
                oid=TransformDynamicOidToCommentOid(dynamicOid, dynamicRid, dynamicType),
                type_=comment.ResourceType(TransformDynamicTypeToCommentType(dynamicType)), 
                page_index=page
            )
        except Exception as e:
            commentLog.error("get_comments " + str(dynamicOid) + " failed, error=" + str(e))
            continue

        # print(dynamicInfo)

        if not dynamicInfo['replies']:
            commentLog.info(f"load {dynamicOid} finished, count={count}")
            break

        if not has_load_upper:
            if dynamicInfo.get('upper'):
                if dynamicInfo['upper'].get('top'):
                    if dynamicInfo['upper']['top'].get('content'):
                        dynamicTime = dynamicInfo['upper']['top']['ctime']
                        # if dynamicTime <= maxCommentReplyTime:
                        #     continue;
                        time_local = time.localtime(dynamicTime)
                        timeStr = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
                        commentRpid = dynamicInfo['upper']['top']['rpid']
                        commentUname = dynamicInfo['upper']['top']['member']['uname']
                        commentUid = dynamicInfo['upper']['top']['mid']
                        commentMsg = dynamicInfo['upper']['top']['content']['message']
                        commentMsg = pymysql.converters.escape_string(value=commentMsg)
                        commentReplyCount = dynamicInfo['upper']['top']['rcount']
                        commentLikeCount = dynamicInfo['upper']['top']['like']
                        try:
                            await myConnect.InsertIntoTable(tablename=str(dynamicOid), rpid=dynamicInfo['upper']['top']['rpid_str'], oid=str(dynamicRid), 
                                    rootid=dynamicInfo['upper']['top']['root'], parentid=dynamicInfo['upper']['top']['root'], uid=str(commentUid), uname=commentUname, 
                                    replyTime=timeStr, rcount=commentReplyCount, rlike=commentLikeCount, comment=commentMsg)
                        except Exception as e:
                            commentLog.error("InsertIntoTable " + str(dynamicOid) + "failed, error=" + str(e))
                            continue
                        
                        dynamicFile.write(f"rpid={commentRpid} name={commentUname}({commentUid})[{timeStr}] rcount={commentReplyCount} like={commentLikeCount} comment=\n{commentMsg}" + '\n\n\n')

            has_load_upper = True

        comments.extend(dynamicInfo['replies'])
        for reply in dynamicInfo['replies']:
            # print(reply)
            dynamicTime = reply['ctime']
            currentMaxCommentReplyTime = dynamicTime if dynamicTime > currentMaxCommentReplyTime else currentMaxCommentReplyTime
            if dynamicTime <= maxCommentReplyTime:
                commentLog.info(f"load {dynamicOid} finished, count={count}")
                isFinish = True
                break

        count += len(dynamicInfo['replies'])
        insertCount += len(dynamicInfo['replies'])
        if count >= dynamicInfo['page']['count']:
            commentLog.info(f"count({count}) >= dynamicInfo['page']['count']({dynamicInfo['page']['count']})")
            isFinish = True
            break

        # print(f"count={count},dynamicInfo['page']['size']={dynamicInfo['page']['size']},dynamicInfo['page']['count']={dynamicInfo['page']['count']}")
        # count += dynamicInfo['page']['size'] if dynamicInfo['page']['count'] > dynamicInfo['page']['size'] else dynamicInfo['page']['count']
        # print(count)
        page += 1

        # if count >= dynamicInfo['page']['count']:
        #     print("count >= dynamicInfo['page']['count'] break")
        #     break
        commentLog.info(f"{dynamicOid}, comments.length={len(comments)}, insertCount={insertCount}, count={count}, repliyCount={len(dynamicInfo['replies'])}, page={page}")
        
        if insertCount >= 1000:
            for cmt in comments:
                dynamicTime = cmt['ctime']
                if dynamicTime <= maxCommentReplyTime:
                    continue;
                time_local = time.localtime(dynamicTime)
                timeStr = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
                commentRpid = cmt['rpid']
                commentUname = cmt['member']['uname']
                commentUid = cmt['mid']
                commentMsg = cmt['content']['message']
                commentMsg = pymysql.converters.escape_string(value=commentMsg)
                commentReplyCount = cmt['rcount']
                commentLikeCount = cmt['like']
                try:
                    await myConnect.InsertIntoTable(tablename=str(dynamicOid), rpid=cmt['rpid_str'], oid=str(dynamicRid), 
                            rootid=cmt['root'], parentid=cmt['root'], uid=str(commentUid), uname=commentUname, 
                            replyTime=timeStr, rcount=commentReplyCount, rlike=commentLikeCount, comment=commentMsg)
                except Exception as e:
                    commentLog.error("InsertIntoTable " + str(dynamicOid) + "failed, error=" + str(e))
                    continue
                
                dynamicFile.write(f"rpid={commentRpid} name={commentUname}({commentUid})[{timeStr}] rcount={commentReplyCount} like={commentLikeCount} comment=\n{commentMsg}" + '\n\n\n')
            comments.clear()
            insertCount = 0
    
        if count == lastCount:
            commentLog.info(f"load {dynamicOid} finished, count={count}")
            isFinish = True
            break

        lastCount = count

    for cmt in comments:
        # print(cmt)
        dynamicTime = cmt['ctime']
        if dynamicTime <= maxCommentReplyTime:
            continue;
        time_local = time.localtime(dynamicTime)
        timeStr = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        commentRpid = cmt['rpid']
        commentUname = cmt['member']['uname']
        commentUid = cmt['mid']
        commentMsg = cmt['content']['message']
        commentMsg = pymysql.converters.escape_string(value=commentMsg)
        commentReplyCount = cmt['rcount']
        commentLikeCount = cmt['like']
        try:
            await myConnect.InsertIntoTable(tablename=str(dynamicOid), rpid=cmt['rpid_str'], oid=str(dynamicRid), 
                    rootid=cmt['root'], parentid=cmt['root'], uid=str(commentUid), uname=commentUname, 
                    replyTime=timeStr, rcount=commentReplyCount, rlike=commentLikeCount, comment=commentMsg)
        except Exception as e:
            commentLog.error("InsertIntoTable " + str(dynamicOid) + "failed, error=" + str(e))
            continue
        
        dynamicFile.write(f"rpid={commentRpid} name={commentUname}({commentUid})[{timeStr}] rcount={commentReplyCount} like={commentLikeCount} comment=\n{commentMsg}" + '\n\n\n')

    dynamicFile.close()
    maxCommentReplyTime = currentMaxCommentReplyTime
    return maxCommentReplyTime

def getCommentReplyLinkage(dynamicid=0, dynamicRpid=0):
    linkage = "https://t.bilibili.com/" + str(dynamicid) + "?tab=2#reply" + str(dynamicRpid)
    return linkage

loop = asyncio.new_event_loop()
loop.run_until_complete(get_all_dynamics())


