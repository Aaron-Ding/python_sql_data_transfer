import pymysql
import pymssql  
import MySQLdb

##import MySQLdb
""" 计划任务包  pip install apscheduler"""
from apscheduler.schedulers.background import BackgroundScheduler


import time
from datetime import datetime
import os
import sys




def loaddata():
	try:
	
			conn=pymssql.connect('INFOBUS1\SQLEXPRESS','sa','aaaaaaaa','teststu') ##建立连接

			cur=conn.cursor() ##创建指针
			loadquery=("select * from [person]") ##sql指令
			cur.execute(loadquery)##进行sql指令查询
			result=cur.fetchall()##全部查询之返回result中
			for row in result:
				FIRST_NAME = row[0]
				LAST_NAME = row[1]
				AGE = row[2]
				SEX = row[3]
				INCOME = row[4]
			
			print(row[0],row[1],row[2],row[3],row[4])
			
			sqlquery=('INSERT INTO SIGNIN  VALUES(%s, %s, %d, %s, %d)')
			cur.execute(sqlquery,row)	
			conn.commit()
			cur.close()  ##关闭连接与指针
			conn.close() 

			print('\n\n nb of ROWS ：'+ 'str(cur.rowcount)') 
		
			return result
	except Exception as err:  
		print(err)	
		
		
		
def restart_program():
  python = sys.executable
  os.execl(python, python, * sys.argv)
		
		
		
if __name__ == '__main__':
	loaddata()
##	print ('start....')
##	print ('.....')
##	time.sleep(3)
##	restart_program()
	
	
	
	