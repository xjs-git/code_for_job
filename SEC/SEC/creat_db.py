#链接mysql数据库，创建数据库表
import pymysql

def create_table():
    #连接本地数据库
    db=pymysql.connect(host='localhost',user='root',password='123456',database='sec_spider',port=3306)

    #创建游标
    cursor=db.cursor()

    #创建company表
    #sql1="INSERT INTO company(cik,cik_url,company_name,state) VALUES ('{}','{}','{}','{}')".format("1","2","1","2")
    sql2 = "INSERT INTO company(cik,cik_url,company_name,state,sic,business_address,mailing_address,formerly) VALUES('1111551155','https://dadaiykhgfaf.com','daihaga','adagah','sgagghsh','scfafvac','sgbxc','sgsfbcc') ON DUPLICATE KEY UPDATE company_name=VALUES(company_name)"
    try:
        #执行SQL语句
        #cursor.execute(sql1)
        db.ping()
        cursor.execute(sql2)
        print(cursor.execute(sql2))
    except Exception as e:
        cursor=db.cursor()
        cursor.execute(sql2)
        print("数据添加失败失败：{}".format(e))
    finally:
        #关闭游标
        db.commit()
        #关闭数据库
        db.close()

if __name__ == '__main__':
    create_table()
