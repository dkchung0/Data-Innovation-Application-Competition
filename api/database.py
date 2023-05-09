import pymysql
class Database(object):
    
    #設定資料庫的連接引數，由于我是在服務器中撰寫的，所以host是localhost
    host = "35.194.177.50"
    user = "root"
    password = "kimo8971"
    
    #類的建構式，引數db為欲連接的資料庫，該建構式實作了資料庫的連接
    def __init__(self,db):
        connect = pymysql.connect(host=self.host,user=self.user,password=self.password,database=db)
        self.cursor = connect.cursor()
    
    #類的方法，引數command為sql陳述句
    def execute(self, command):
        try:
            #執行command中的sql陳述句
            self.cursor.execute(command)
        except Exception as e:
            return e
        else:
            #fetchall()回傳陳述句的執行結果，并以元組的形式保存
            return self.cursor.fetchall()

if __name__ == "__main__":
    
    sql = Database("data_contest")

    result = sql.execute("""SELECT Townships FROM Population""")
    print(result)