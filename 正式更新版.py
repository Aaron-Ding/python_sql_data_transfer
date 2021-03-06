import pymysql
import pymssql  
import MySQLdb
import operator 

from apscheduler.schedulers.background import BackgroundScheduler


import time
from datetime import datetime
import os
import sys
import datetime
import calendar
import time

def loaddata():
	try:
	
			conn=pymysql.connect('52.60.62.172','ding','ding','ci_sinorama_oa') ##建立连接
			##low =(490, datetime.date(2018, 1, 24), datetime.time(10, 0, 6))
				  # (490, datetime.date(2018, 1, 23), datetime.time(10, 00, 6))
			##此处需要一个for 循环提取low中的每一个时间元素


			#print(i)
			row= {}  ##把一串时间放入并不在是一个时间了
			row[0]=''
			row[1]= low[i][0]  ##前面需要自动补零
			row[1] = "%04d" %row[1]
			
			#row[1] = row[1].zfill(4)				
			row[2]=low[i][1]
			row[2] = row[2].strftime("%Y-%m-%d")
			#print(row[2])
			row[3]=low[i][2]
			#print(row[3])
			row[3] = row[3].strftime("%H:%M:%S")
			#print(row[3])
			in_time = row[3]
			cur=conn.cursor() ##创建指针
			loadquery = ("select id,dept_code from oac_user where finance_no= %s")
			cur.execute(loadquery,row[1])       ##根据signmachine 的emp—no 查找10开头的id 和 deptcode
			row[4]=cur.fetchone()		##oac_user information fechted by row[1]
			#print(row[4])
			#print(row[1])
			loadquery5 = ('select in_time from sign_recordtest where emp_no=%s and sign_date =%s')   ##..........................................................
			rowdata5 = (row[4][0],row[2])
			#print(row[2])
			#print(rowdata5)
			cur.execute(loadquery5,rowdata5)
			row[7] = cur.fetchone()
			#print(row[7])
			
			if (row[7] is None):
				#print(row[7])
				#print("insert into emp_no")
				loadquery = ("""INSERT INTO sign_recordtest (emp_no,dept_code,sign_date)  VALUES (%s,%s,%s)""")
				rowdata1 = (row[4][0],row[4][1],row[2])
				cur.execute(loadquery,rowdata1)
				conn.commit()
			loadquery1 = ('select in_time,out_time from sign_recordtest where emp_no= %s and sign_date = %s')
			rowdata = (row[4][0],row[2])

			#print(row[4][0],row[2])
			cur.execute(loadquery1,rowdata)
			row[5] = cur.fetchone()  
			#print("1111111")
			##取出当天所有打卡时间，可能是tuple 不是dict
			row[6] = cur.fetchone()
			#print(row[10])   ##查找到现在已有的in_time 值
			#for row[5] in row[10]:
				#row[5] = row[10][1]
			#print(row[5][0])

			#print(row[6])

			if (row[5][0] is None):  ##数据结构不稳定
				#print("true")
				loadquery10 =  ("update sign_recordtest SET in_time = %s" "where  emp_no= %s and sign_date = %s")
				rowdata5 = (row[3],row[4][0],row[2])
				cur.execute(loadquery10,rowdata5)
				conn.commit()

				
			now = datetime.datetime.now()
			in_time = row[5][0]
			in_time = str(in_time) ############
			in_time2 = row[5][1]
			out_time = row[3]
			
			in_time_string = now.strftime("%Y-%m-%d") + " " + in_time  ## datetime to  str

			#in_time2_string = now.strftime("%Y-%m-%d") + " " + in_time2 
			
			out_time_string = now.strftime("%Y-%m-%d") + " " + out_time
			#print(out_time_string) 
			s_out_time = time.mktime(time.strptime(out_time_string,'%Y-%m-%d %H:%M:%S'))
			s_in_time = time.mktime(time.strptime(in_time_string,'%Y-%m-%d %H:%M:%S'))
			#s_in_time2 = time.mktime(time.strptime(in_time2_string,'%Y-%m-%d %H:%M:%S'))
			
			print(s_in_time)
			print(s_out_time)
			if ((float(s_out_time)) > float(s_in_time) and (row[5][1] is None) and row[5][0] is not None) :	
				#print(row[5][1] is None)
				#print("true")
				loadquery2 = ("update sign_recordtest SET out_time = %s" "where sign_date = %s and emp_no = %s" ) 
				rowdata = (row[3],row[2],row[4][0])
				cur.execute(loadquery2,rowdata)
				conn.commit()
				
			elif ((float(s_out_time)) > float(s_in_time) and (row[5][1] is not None) and row[5][0] is not None and row[6] is None) :
				#print("insert a new conlum")
				loadquery2 = ("INSERT INTO sign_recordtest" "(emp_no,dept_code,sign_date,in_time)"" VALUES (%s,%s,%s,%s)"
     )
				rowdata1 = (row[4][0],row[4][1],row[2],row[3])
				cur.execute(loadquery2,rowdata1)
				conn.commit()  ##总是提前与最后一个时间进入前插入一条错误的行

				
			elif ((row[6][1] is None) and (row[5][0] is not None)):
				#print("off work time")
				loadquery2 = ("update sign_recordtest SET out_time = %s" "where sign_date = %s and in_time = %s" )  ##update了两次，需要修改
				rowdata = (row[3],row[2],row[6][0])
				cur.execute(loadquery2,rowdata) 
				conn.commit()  ## 最后一个时间戳有问题



			#rowdata = (row[4][0],row[2])
			#cur.execute(loadquery1,rowdata)
			#row[6] = cur.fetchone()
			#if row[6][1] != 'NUll':
				#loadquery3 = ("INSERT INTO sign_recordtest" "(emp_no,dept_code,sign_date,in_time)"" VALUES (%d,%d,%s,%s)",([row[4][0],row[4][1],row[2],row[3]]),
     #)




														 
			#loadquery2 = ()				##拿新的in_time与已有的in_time进行比较


			#cur.executemany(
    #"INSERT INTO sign_recordtest" "(emp_no,dept_code,sign_date,in_time)"" VALUES (%d,%d,%s,%s)",
    #[(row[4][0],row[4][1],row[2],row[3]),
    # ])		
			#conn.commit()
			cur.close()  ##关闭连接与指针
			conn.close() 

			#print('\n\n nb of ROWS ：'+ 'str(cur.rowcount)') 
		
			#return result
	except Exception as err:  
		print(err)	
def insertMySQL():
	#MYSQL 数据库联机
	hostMySQL = '52.60.62.172'
	userMySQL = 'ding'
	pswMySQL = 'ding'
	dbMySQL = 'ci_sinorama_oa'
	try:
		row = {}
		row[0] = low[i][0]
		row[1] = low[i][1]
		row[1] = row[1].strftime("%Y-%m-%d")
		
		row[2]=low[i][2]
		
		row[2] = row[2].strftime("%H:%M:%S")

		rowData = (row[0],row[1],row[2])
		#print(rowData)
		
		qMySQL = ("INSERT INTO sign_machinetest "
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
		#print ('*************************************************************************************************************')
		#start=datetime.now()
		curMySQL.execute(qMySQL,rowData)  
		#print (datetime.now()-start)
		#print ('*************************************************************************************************************')

		#关闭联机
		connMySQL.commit()
		curMySQL.close()
		connMySQL.close()  
	except Exception as err:
		print(err)
if __name__ == '__main__':
	#now = datetime.datetime.now()
	
			#loadquery = ('select [id],[dept_code] from [dbo].[oac_user] where finance_no= %d')
	low = [(125, datetime.date(2018, 3, 12), datetime.time(16, 35, 4)), (125, datetime.date(2018, 3, 12), datetime.time(17, 6, 28)), (138, datetime.date(2018, 3, 12), datetime.time(17, 9, 43)), (152, datetime.date(2018, 3, 12), datetime.time(16, 55, 58)), (231, datetime.date(2018, 3, 12), datetime.time(17, 0, 15))]
	print(len(low))
	number = len(low)
	for i in range (0,number):
		loaddata()
		print("finish insert into sign_recordtest")
		insertMySQL()
		#print(low[i])
		print("finish insert into sign_machinetest")
		#print(i)