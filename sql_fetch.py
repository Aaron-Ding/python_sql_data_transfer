import pymysql
import pymssql  
conn=pymysql.connect('52.11.242.182','root','sino3388','vmail')
##建立连接
cur=conn.cursor()
print("...............................")
loadquery = ("select username from mailbox ")
cur.execute(loadquery)  
result = cur.fetchall()
print(result)