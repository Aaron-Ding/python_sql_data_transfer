import pymysql
import pymssql  
#import MySQLdb
import operator 

from apscheduler.schedulers.background import BackgroundScheduler


import time
from datetime import datetime
import os
import sys
import datetime
import calendar
import time

listodo = [(179, datetime.datetime(2018, 1, 25, 13, 53, 6)), (239, datetime.datetime(2018, 1, 25, 13, 53, 4)), (262, datetime.datetime(2018, 1, 25, 13, 52, 8)),
(272, datetime.datetime(2018, 1, 25, 13, 53, 22)), (383, datetime.datetime(2018, 1, 25, 13, 51, 56)), (395, datetime.datetime(2018, 1, 25, 13, 52, 34))]
rowcount = 4
low = []
for i in listodo :
	y = (i[0],i[1].date(),i[1].time())
	low.append(y)
print(low)


