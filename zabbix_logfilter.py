#!/usr/bin/python
# coding:utf-8
from influxdb import client as influxdb
import MySQLdb
import datetime
from warnings import filterwarnings
filterwarnings('ignore', category = MySQLdb.Warning)

def mysql_dml(ip,username,password,sql):    
    try:
        conn = MySQLdb.connect(host=ip,user=username,passwd=password,port=53306,connect_timeout=100)
        #conn = MySQLdb.connect(host='59.53.92.181',user='root',passwd='dbksrootpwd',port=33639,connect_timeout=100)
        conn.select_db('information_schema')
        cur = conn.cursor()         
        count = cur.execute(sql)
        conn.commit()         
        if count == 0:
            result = 0
        else:
            result = cur.fetchall()
        return result
        cur.close()
        conn.close()
    except Exception,e:
        print "mysql dml error:" ,e
        

def write_errlog_influxdb(database,table,udb_name,query_time,rows_sent,rows_examined,sql_text):
    db = influxdb.InfluxDBClient('127.0.0.1', 18086, 'root', 'kashuoops741258963', '%s'%(database))
    json_body = [{"measurement": table,"tags": {"db": udb_name,"query_time": query_time,"rows_sent":rows_sent,"rows_examined":rows_examined},"fields": {"sql_text": sql_text}}]
    try:
        db.write_points(json_body)
    except Exception,e:
        print 'influxdb write_points error:',e

interval_time = "'"+(datetime.datetime.now() - datetime.timedelta(minutes=60*24)).strftime("%Y-%m-%d %H:%M:%S")+"'"
        
row1 = mysql_dml('10.9.65.30','root',"Sxy%2rqajeh9$qFjnI@y","select db,time_to_sec(query_time),rows_sent,rows_examined,sql_text from mysql.slow_log where start_time>=%s"%(interval_time))
row2 = mysql_dml('10.9.161.171','root',"Starbase#1419#kzhou@BJ@SH@SZ","select db,time_to_sec(query_time),rows_sent,rows_examined,sql_text from mysql.slow_log where start_time>=%s"%(interval_time))
row3 = mysql_dml('10.9.118.16','root',"Starbase#1419#kzhou@BJ@SH@SZ","select db,time_to_sec(query_time),rows_sent,rows_examined,sql_text from mysql.slow_log where start_time>=%s"%(interval_time))
row = ()
if isinstance(row1,tuple):
    row = row1
if isinstance(row2,tuple):
    row = row + row2
if isinstance(row3,tuple):
    row = row + row3
print row
if len(row)== 0:
    print 'no slow query to process!'
else:
    for x in range(len(row)):
        udb_name = row[x][0]
        query_time = row[x][1]
        rows_sent = row[x][2]
        rows_examined = row[x][3]
        sql_text = row[x][4]
        write_errlog_influxdb('mysql', 'slow_query_statistics', udb_name, query_time, rows_sent, rows_examined, sql_text)
