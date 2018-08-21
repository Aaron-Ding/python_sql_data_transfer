import pymysql
import pymssql  
import MySQLdb
import types 
##import MySQLdb
""" 计划任务包  pip install apscheduler"""
from apscheduler.schedulers.background import BackgroundScheduler

import time
from datetime import datetime
import os
import sys
import datetime
import calendar


def loaddata():
	try:
	
			conn=pymssql.connect('INFOBUS1\SQLEXPRESS','sa','aaaaaaaa','oa') ##建立连接
			##low =(490, datetime.date(2018, 1, 24), datetime.time(10, 0, 6))
				  # (490, datetime.date(2018, 1, 23), datetime.time(10, 00, 6))
			##此处需要一个for 循环提取low中的每一个时间元素


			print(i)
			row= {}  ##把一串时间放入并不在是一个时间了
			row[0]=''
			row[1]= low[i][0]  ##前面需要自动补零
			#row[1] = row[1].zfill(4)				
			row[2]=low[i][1]
			row[3]=low[i][2]
			row[3] = row[3].strftime("%H:%M:%S")
			print(row[3])
			in_time = row[3]
			cur=conn.cursor() ##创建指针
			loadquery = ('select [id],[dept_code] from [dbo].[oac_user] where finance_no= %d')
			cur.execute(loadquery,row[1])       ##根据signmachine 的emp—no 查找10开头的id 和 deptcode
			row[4]=cur.fetchone()		##oac_user information fechted by row[1]
			#print(row[4])
			loadquery5 = ('select [in_time] from [dbo].[sign_record] where emp_no= %d and sign_date = %s')
			rowdata5 = (row[4][0],row[2])
			cur.execute(loadquery5,rowdata5)
			row[7] = cur.fetchone()
			print(row[7])
			if (row[7] is None):
				print(row[7])
				print("insert into emp_no")
				loadquery = ("""INSERT INTO sign_record (emp_no,dept_code,sign_date)  VALUES (%d,%s,%s)""")
				rowdata1 = (row[4][0],row[4][1],row[2])
				cur.execute(loadquery,rowdata1)
				conn.commit()
			loadquery1 = ('select [in_time],[out_time] from [dbo].[sign_record] where emp_no= %d and sign_date = %s')
			rowdata = (row[4][0],row[2])

			print(row[4][0],row[2])
			cur.execute(loadquery1,rowdata)
			row[5] = cur.fetchone()  
			print("1111111")
			##取出当天所有打卡时间，可能是tuple 不是dict
			row[6] = cur.fetchone()
			#print(row[10])   ##查找到现在已有的in_time 值
			#for row[5] in row[10]:
				#row[5] = row[10][1]
			print(row[5])

			print(row[6])

			if (row[5][0] is None):  ##数据结构不稳定
				print("true")
				loadquery10 =  ("update sign_record SET in_time = %s" "where  emp_no= %d and sign_date = %s")
				rowdata5 = (row[3],row[4][0],row[2])
				cur.execute(loadquery10,rowdata5)
				conn.commit()

				
			now = datetime.datetime.now()
			in_time = row[5][0]
			in_time2 = row[5][1]
			#print(row[5][1])
			out_time = row[3]
			print(out_time)
			in_time_string = now.strftime("%Y-%m-%d") + " " + in_time
			#in_time2_string = now.strftime("%Y-%m-%d") + " " + in_time2 
			out_time_string = now.strftime("%Y-%m-%d") + " " + out_time
			print(out_time_string) 
			s_out_time = time.mktime(time.strptime(out_time_string,'%Y-%m-%d %H:%M:%S'))
			s_in_time = time.mktime(time.strptime(in_time_string,'%Y-%m-%d %H:%M:%S'))
			#s_in_time2 = time.mktime(time.strptime(in_time2_string,'%Y-%m-%d %H:%M:%S'))
			
			print(s_in_time)
			print(s_out_time)
			if ((float(s_out_time)) > float(s_in_time) and (row[5][1] is None) and row[5][0] is not None) :	
				#print(row[5][1] is None)
				print("true")
				loadquery2 = ("update sign_record SET out_time = %s" "where sign_date = %s and emp_no = %s" ) 
				rowdata = (row[3],row[2],row[4][0])
				cur.execute(loadquery2,rowdata)
				conn.commit()
				
			elif ((float(s_out_time)) > float(s_in_time) and (row[5][1] is not None) and row[5][0] is not None and row[6] is None) :
				print("insert a new conlum")
				loadquery2 = ("INSERT INTO sign_record" "(emp_no,dept_code,sign_date,in_time)"" VALUES (%d,%d,%s,%s)"
     )
				rowdata1 = (row[4][0],row[4][1],row[2],row[3])
				cur.execute(loadquery2,rowdata1)
				conn.commit()  ##总是提前与最后一个时间进入前插入一条错误的行

				
			elif ((row[6][1] is None) and (row[5][0] is not None)):
				print("off work time")
				loadquery2 = ("update sign_record SET out_time = %s" "where sign_date = %s and in_time = %s" )  ##update了两次，需要修改
				rowdata = (row[3],row[2],row[6][0])
				cur.execute(loadquery2,rowdata) 
				conn.commit()  ## 最后一个时间戳有问题



		
			cur.close()  ##关闭连接与指针
			conn.close() 

			#print('\n\n nb of ROWS ：'+ 'str(cur.rowcount)') 
		
			#return result
	except Exception as err:  
		print(err)	

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
		numberofrow = str(curMSSQL.rowcount)
		# 返回结果
		return (rMSSQL,numberofrow)
	except Exception as err:  
		print(err)

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
				
def job_sch():
	#get rows with flag =0
	x = loadMSSQL()
	
	if not low:
		print("there is no data need to be sync")
	else: 
		print ('there is some data need to be sync , please wait until finish ')
		#更改为mysql 数据格式
		low = []	
		for m in x[0] :
			y = (m[0],m[1].date(),m[1].time())
			low.append(y)

		insertMySQL(low)
		Print("insert into sign machine")
		for i in range (0,x[1]):
			loaddata()



		updateMSSQL(x)
		print("update the flag to 1")



if __name__ == '__main__':
	#now = datetime.datetime.now()
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
	