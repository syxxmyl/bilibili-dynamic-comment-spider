import pymysql
from loguru import logger


mysqlLog = logger.bind(user="mysql")
mysqlLog.remove()
mysqlLog.add(
    "mysqlErr.log",
    colorize=True,
    backtrace=True,
    diagnose=True,)

class MyMysqlConnect:
    def __init__(self, host='localhost', port=3306, user='root', password='123456'):

        self.conn = None
        self.cursor = None
        
        try:
            self.conn = pymysql.connect(host=host, port=port, user=user, password=password)
            self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        except Exception as e:
            mysqlLog.error("mysql connect init failed, error=" + str(e))

    def __del__(self):
        self.conn.close()

    def CreateDatabase(self, database : str ='comment') -> bool:
        try:
            sql = "CREATE DATABASE IF NOT EXISTS " + database + " DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_general_ci"
            self.cursor.execute(sql)
        except Exception as e:
            mysqlLog.error("mysql create Database " + database + "failed, error=" + str(e))
            return False
        return True

    def UseDatabase(self, database : str = 'comment') -> bool:
        try:
            sql = "USE " + database
            self.cursor.execute(sql)
        except Exception as e:
            mysqlLog.error("mysql create/use Database " + database + "failed, error=" + str(e))
            return False
        return True

    def CreateCommentTable(self, tablename: str) -> bool:
        try:
            sql = """create table if not exists comment_%s (
                `comment_rpid` VARCHAR(30) PRIMARY KEY NOT NULL,
                `comment_oid` VARCHAR(30) NOT NULL,
                `comment_root_id` VARCHAR(30) NOT NULL,
                `comment_parent_id` VARCHAR(30) NOT NULL,
                `user_mid` VARCHAR(20) NOT NULL,
                `user_name` VARCHAR(20) NOT NULL,
                `reply_time` DATETIME NOT NULL,
                `reply_count` INT NOT NULL,
                `reply_like` INT NOT NULL,
                `comment_str` VARCHAR(10000) NOT NULL
            )""" %(tablename)
            self.cursor.execute(sql)
        except Exception as e:
            mysqlLog.error("mysql create commentTable " + tablename + "failed, error=" + str(e))
            return False
        return True

    async def InsertIntoTable(self, tablename: str, rpid: str, oid: str, rootid: str, parentid: str, 
        uid: str, uname: str, replyTime: str, rcount: int, rlike: int, comment: str) -> bool:
        try:
            sql = """insert into comment_%s(comment_rpid, comment_oid, comment_root_id, comment_parent_id, 
            user_mid, user_name, reply_time, reply_count, reply_like, comment_str) 
            values('%s','%s','%s','%s','%s','%s','%s',%d,%d,'%s');""" %(tablename, rpid, oid, rootid, parentid, uid, uname, replyTime, rcount, rlike, comment)
            
            # print(sql)
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            mysqlLog.error(sql + '\n' + "mysql insert commentTable " + tablename + " failed, rpid= " + rpid + ", error=" + str(e))
            self.conn.rollback()
            return False
        return True

# myConnect = MyMysqlConnect()
# myConnect.CreateDatabase(database="comment")
# myConnect.UseDatabase(database="comment")
