""" 数据库驱动 pip install pymysql ,pymssql """
import pymysql
import pymssql  

""" 计划任务包  pip install apscheduler"""
from apscheduler.schedulers.background import BackgroundScheduler


import time
from datetime import datetime
import os
import sys

# function loadMSSQL()
# input -> empty
# outpot -> SQL query results ->  select rows where flag = 1 
#
def loadMSSQL():
	#DB setting paramatres
	hostMSSQL = 'ewqINFOBUS1\SQLEXPRESSINFOBUS1\SQLEXPRESS' # hostname -> 192.168.1.196\SQLEXPRESS for onlinr login 
	userMSSQL = 'oatest'
	pswMSSQL = 'zhangyhhhaoyu'
	dbMSSQL = 'face'
	try:
		#查询语句	
		qMSSQL =  'SELECT [DIN],[Clock] FROM [dbo].[ras_AttRecord] where flag = 0'
		# 建立数据库连接
		connMSSQL = pymssql.connect(
									host = hostMSSQL,
									user = userMSSQL,
									password = pswMSSQL,
									database = dbMSSQL	
									)
		
		curMSSQL=connMSSQL.cursor()
		#执行查询
		curMSSQL.execute(qMSSQL)  
		rMSSQL=curMSSQL.fetchall() 
		#关闭连接
		curMSSQL.close()  
		connMSSQL.close() 
		print('\n\n nb of ROWS ：'+ str(curMSSQL.rowcount))  
		# 返回结果
		return rMSSQL
	except Exception as err:  
		print(err)
        

		
#function insertMySQL( rowData)
#input SQL SERVER results type  rowData -> [(tuples),] 
#output empty
#result -> insert datas to mysql oa system		
def insertMySQL(rowData):
	#MYSQL 数据库联机
	hostMySQL = '52.60.62.172'
	userMySQL = 'didfng'
	pswMySQL = 'didfng'
	dbMySQL = 'ci_dfsinorama_oa'
	try:
		qMySQL = ("INSERT INTO sign_machine "
				  "(emp_no, sign_date, in_time)"
                  "VALUES (%s, %s, %s)")

		connMySQL = pymysql.connect(
									host = hostMySQL,
									user = userMySQL,
									password = pswMySQL,
									database = dbMySQL	
									)
		curMySQL=connMySQL.cursor()
		
		#插入数据,统计执行时间
		print ('*************************************************************************************************************')
		start=datetime.now()
		curMySQL.executemany(qMySQL,rowData)  
		print (datetime.now()-start)
		print ('*************************************************************************************************************')

		#关闭联机
		connMySQL.commit()
		curMySQL.close()
		connMySQL.close()  
	except Exception as err:
		print(err)

#function updateMSSQL( rowData)
#input SQL SERVER results type  rowData -> [(tuples),] 
#output empty
#result -> updata data flag to 1		
def updateMSSQL(rowData):

	hostMSSQL = 'INFOBUS1\SQLEXPRESS'
	userMSSQL = 'oatest'
	pswMSSQL = 'zhangyaoyu'
	dbMSSQL = 'face'
	try:
		qMSSQL =  ('UPDATE [dbo].[ras_AttRecord] SET flag = 1 WHERE DIN =%s  and Clock = %s')
		connMSSQL = pymssql.connect(
									host = hostMSSQL,
									user = userMSSQL,
									password = pswMSSQL,
									database = dbMSSQL	
									)
		curMSSQL=connMSSQL.cursor()
		#更新数据,统计执行时间	
		print ('*************************************************************************************************************')
		start=datetime.now()
		curMSSQL.executemany(qMSSQL,rowData)  	
		print (datetime.now()-start)
		print ('*************************************************************************************************************')
		#关闭联机
		connMSSQL.commit()
		curMSSQL.close()  
		connMSSQL.close()  

	except Exception as err:  
		print(err)
				
#计划任务程序
def job_sch():
	#get rows with flag =0
	x = loadMSSQL()

	if not x:
		print("Il y a plus de donnees")
	else: 
		print ('Il y a des donnees non sync , commencer a traiter les donnees ')
		#更改为mysql 数据格式
		listdo = []	
		for i in x :
			y = (i[0],i[1].date(),i[1].time())
			listdo.append(y)
			
		insertMySQL(listdo)
		print('Insert to Mysql table')
		updateMSSQL(x)
		print('update MSSQL flag to 1')
		
		

if __name__ == "__main__":
	
	#计划任务调度程
	scheduler = BackgroundScheduler()
	scheduler.add_job(job_sch, 'interval', minutes=3)
	scheduler.start() 

	print('Script running go for an coffee,Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
	
	try :
        # This is here to simulate application activity (which keeps the main thread alive).
		while True:
			time.sleep(5)
	except (KeyboardInterrupt, SystemExit):
		# Not strictly necessary if daemonic mode is enabled but should be done if possible
		scheduler.shutdown()
			
