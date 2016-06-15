import boto
import urllib2
import csv
from boto import rds
import MySQLdb
import time
import StringIO
import random

aws_access_key_id = 'L-O-L'
aws_secret_access_key = 'L-O-L'

s3 = boto.connect_s3()
bucket = s3.create_bucket('firstawsbucketj')                                              # bucket names must be unique
print 'Bucket created'
key = bucket.new_key('Example/myQueryFile.csv')                                       # this key is used to store the data in fashion similar to creating folders

key.set_contents_from_filename('D:/Edu/MS/Semester 3 Summer/Assignment4/testCSV.csv')    
key.set_acl('public-read')                                                                # data available for public-read



db = MySQLdb.connect(host="awsinstance1.czxww7h56w8i.us-west-2.rds.amazonaws.com",
                     user="sjeet",
                     passwd="Sjeetandroid",
                     db="myRDS1")

cursor = db.cursor()

reader = csv.reader(StringIO.StringIO(key.get_contents_as_string()), csv.excel)
#reader = key.get_contents_as_string()
#start = time.time()

data=[]
'''
for i in reader:
	data.append(i)     
del data[0]
		
for i in data:
	  #print(i)
	  #print("\n"+reader)
      cursor.execute('INSERT INTO queryTable(time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14] ) )
end = time.time()'''
#INSERT INTO  accTable(CASE_NUMBER, BARRACK, ACC_DATE, ACC_TIME, ACC_TIME_CODE, DAY_OF_WEEK, ROAD, INTERSECT_ROAD, DIST_FROM_INTERSECT, DIST_DIRECTION, CITY_NAME, COUNTY_CODE, COUNTY_NAME, VEHICLE_COUNT, PROP_DEST, INJURY, COLLISION_WITH_1, COLLISION_WITH_2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14], i[15], i[16], i[17]) )
#time_taken = end-start

#print 'Time taken to put data from AWS Storage into RDS Instance is: '+str(time_taken)+' seconds.'

queryList=[]

queryList.append("SELECT * FROM queryTable WHERE mag<=2")
queryList.append("SELECT * FROM queryTable WHERE place like 'cal%'")
queryList.append("SELECT * FROM queryTable WHERE depth>5")

queryST = time.time()
for i in range(0, 1000):
	rand = random.randint(0,2)
	cursor.execute(queryList[rand])
queryET = time.time();

time_consumed = queryET - queryST

print 'Time taken is: '+str(time_consumed)+'seconds for running 1000 queries. '

#----------------------------------------------------------------------------------------
queryST1 = time.time()
for i in range(0, 5000):
	rand = random.randint(0,2)
	cursor.execute(queryList[rand])
queryET1 = time.time();

time_consumed1 = queryET1 - queryST1

print 'Time taken is: '+str(time_consumed1)+'seconds for running 5000 queries. '

#-----------------------------------------------------------------------------------------
queryST2 = time.time()
for i in range(0, 10000):
	rand = random.randint(0,2)
	cursor.execute(queryList[rand])
queryET2 = time.time();

time_consumed2 = queryET2 - queryST2

print 'Time taken is: '+str(time_consumed2)+'seconds for running 10000 queries. ' 	

db.commit()







