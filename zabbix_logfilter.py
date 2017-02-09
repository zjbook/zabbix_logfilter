#!/usr/bin/env python
# coding:utf-8
# version:2.1.1
from influxdb import client as influxdb
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import MySQLdb
import datetime
import re
import hashlib
import urllib
import urllib2
import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

# mail
mail_user = "monitor@kashuo.com"
mail_pass = "1q2w3e4R"
smtp_server = "smtp.kashuo.com"
to_list = []

def send_mail(username,password,smtp_server,to_list,sub,content):
    print to_list
    me = "monitor@kashuo.com"
    msg = MIMEMultipart()
    msgText = MIMEText(content,_subtype="html",_charset="utf8")
    msg["Subject"] = sub
    msg["From"] = me
    msg["To"] = ";".join(to_list)
    msg.attach(msgText)
    try:
        server = smtplib.SMTP()
        server.connect(smtp_server)
        server.login(username,password)
        server.sendmail(me,to_list,msg.as_string())
        server.close()
        print "send mail Ok!"
    except Exception, e:
        print e

def send_wechat(sub,content):
    try:
        values={}
        values['title'] = sub
        values['content'] = content
        data = urllib.urlencode(values) 
        #url = "http://sc.ftqq.com/SCU237T9a4fddbeef403320c73db5b61752837f55f0f5077044d.send"
        url = "http://222.92.13.226:29000/wechat/push?appId=suzhouaizhi"
        #geturl = url + "?"+data
        request = urllib2.Request(url,data)
        urllib2.urlopen(request)
    except Exception, e:
        print e

def mysql_dml(sql):    
    try:
        conn = MySQLdb.connect(host='127.0.0.1',user='zabbix_user',passwd='kashuoops741258963',port=33639,charset="utf8",connect_timeout=100)
        conn.select_db('zabbix')
        cur = conn.cursor()
        cur.execute("SET NAMES utf8");         
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

def write_errlog_influxdb(database,table,host,count,value):
    db = influxdb.InfluxDBClient('127.0.0.1', 18086, 'root', 'kashuoops741258963', '%s'%(database))
    json_body = [{"measurement": table,"tags": {"host": host,"count": count},"fields": {"value": value}}]
    try:
        db.write_points(json_body)
    except Exception,e:
        print 'error:',e

def query_host(itmeid_in):
    conn = MySQLdb.connect(host='127.0.0.1',user='zabbix_user',passwd='kashuoops741258963',port=33639,charset="utf8",connect_timeout=20)
    conn.select_db('zabbix')
    with conn:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SET NAMES utf8");
        cur.execute("select hostid,name from items where itemid=%s"%(itmeid_in))
        row = cur.fetchone()
        hostid = row["hostid"]
        itemname = row["name"]
        cur.execute("select name from hosts where hostid=%s"%hostid)
        row = cur.fetchone()
        hostname = row["name"]
        return hostname,itemname

#获得上次捞取history_log表数据的标记位置，如果pre_clock为空，则取5分钟前的clock
def query_pre_clock():
    conn = MySQLdb.connect(host='127.0.0.1',user='zabbix_user',passwd='kashuoops741258963',port=33639,charset="utf8",connect_timeout=20)
    conn.select_db('zabbix')
    with conn:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SET NAMES utf8");
        cur.execute("select max(pre_clock) from zm_log_error_clock")
        row = cur.fetchone()
        #print row["max(pre_clock)"]
        if row["max(pre_clock)"] == None:
            interval_time = "'"+(datetime.datetime.now() - datetime.timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")+"'"
            print "interval_time:%s"%(interval_time)
            cur.execute("select min(clock) from history_log where clock >= unix_timestamp(%s)"%(interval_time))
            row1 = cur.fetchone()
            pre_clock = row1["min(clock)"]
            print '5min before pre_clock:',pre_clock
        else:
            pre_clock = row["max(pre_clock)"]
        return pre_clock

#将zabbix history_log表数据捞取出来逐一过滤，存入zm_log_error_filter表
def logfilter(pre_clock):
    conn = MySQLdb.connect(host='127.0.0.1',user='zabbix_user',passwd='kashuoops741258963',port=33639,charset="utf8",connect_timeout=20)
    conn.select_db('zabbix')
    with conn:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SET NAMES utf8");
        cur.execute("truncate table zm_log_error_filter")
        print 'input pre_clock:',pre_clock
        cur.execute("select max(clock) from history_log where clock >= %s"%(pre_clock))
        row1 = cur.fetchone()
        current_clock = row1["max(clock)"]
        print 'current_clock:',current_clock
        if pre_clock < current_clock:
            cur.execute("select itemid,value,ns from history_log where clock >= %s and clock < %s"%(pre_clock,current_clock))
        else:
            return 1
        numrows = int(cur.rowcount)
        for x in range(numrows):
            row = cur.fetchone()
            print "处理前报错内容为:",row["value"]
            itemid = row["itemid"]
            ns = row["ns"]
            #财务
            if 'insertSelective' in row["value"]:
                continue
            if '[ERROR]:此用户无权限访问' in row["value"]:
                continue
            #知店
            if '商户[11]没有积分规则' in row["value"]:
                continue
            if 'com.kashuo.crm.utils.DesUtil 68 ERROR: DES加密出错' in row["value"]:
                continue
            if 'com.kashuo.crm.cache.BankCardCacheManager 106 ERROR: 缓存中获取银行卡' in row["value"]:
                continue
            if 'com.kashuo.crm.logic.kf.CommonLogic 147 ERROR: level promotion setting 的level[2]无值' in row["value"]:
                continue
            if '缓存中获取银行卡[11]的mobile' in row["value"]:
                continue
            if 'com.kashuo.crm.config.JerseyJacksonExceptionSupport 87 ERROR: {"code":"INVALID_TOKEN","msg":"token无效"}' in row["value"]:
                continue
            if 'AfterRequestToResponse 49 INFO' in row["value"]:
                continue
            str1 = re.sub('[\[]{0,1}[\d]{4}-[\d]{2}-[\d]{2} [\d]{2}:[\d]{2}:[\d]{2}[\,]{0,1}[\d]{0,3}[\]]{0,1}\s*','',row["value"])
            str2 = re.sub("\/{0,1}([1]?\d\d?|2[0-4]\d|25[0-5])\.([1]?\d\d?|2[0-4]\d|25[0-5])\.([1]?\d\d?|2[0-4]\d|25[0-5])\.([1]?\d\d?|2[0-4]\d|25[0-5]) [\,]{0,1}\s*",'xxx.xxx.xxx.xxx ',str1)            
            str_format = "'"+str2+"'"
            value_md5 = "'"+md5(str2)+"'"
            print "处理后报错内容为:",str_format
            mysql_dml("insert into zm_log_error_filter(itemid,value_md5,value,ns) values (%s,%s,%s,%s)"%(itemid,value_md5,str_format,ns))
        mysql_dml("insert into zm_log_error_clock(pre_clock) values (%s)"%(current_clock))
        return 0
        
def md5(strs):
    m = hashlib.md5()
    m.update(strs)
    return m.hexdigest()

def process_maillist(itemname):
    if 'wxhb' in itemname:
        to_list = ["zoujianbo@kashuo.com","pengcunhua@kashuo.com"]
    elif 'finance' in itemname:
        to_list = ["zoujianbo@kashuo.com","honggang@kashuo.com"]
    elif 'jinshang' in itemname or 'zhidian' in itemname or 'zdcrm' in itemname:
        to_list = ["zoujianbo@kashuo.com","zhaochencheng@kashuo.com","yangyincong@kashuo.com"]
    elif 'kop' in itemname:
        to_list = ["zoujianbo@kashuo.com","miaojiongwei@kashuo.com"]
    elif 'auth' in itemname:
        to_list = ["zoujianbo@kashuo.com","zhuminghua@kashuo.com"]
    elif 'kap' in itemname and 'kapconfig' not in itemname:
        to_list = ["zoujianbo@kashuo.com","douhuatong@kashuo.com"]
    elif 'kapconfig' in itemname:
        to_list = ["zoujianbo@kashuo.com","shiping@kashuo.com"]
    else:
        to_list = ["zoujianbo@kashuo.com"]
    return to_list

#main
try:
    conn = MySQLdb.connect(host='127.0.0.1',user='zabbix_user',passwd='kashuoops741258963',port=33639,charset="utf8",connect_timeout=20)
    conn.select_db('zabbix')
    with conn:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SET NAMES utf8");
        cur1 = conn.cursor(MySQLdb.cursors.DictCursor)
        cur1.execute("SET NAMES utf8");
        cur2 = conn.cursor(MySQLdb.cursors.DictCursor)
        cur2.execute("SET NAMES utf8");
        pre_clock = query_pre_clock()
        #print 'query_pre_clock() is done'
        logfilter_return = logfilter(pre_clock)
        #print 'logfilter(pre_clock) is done'
        #print 'logfilter_return:',logfilter_return
        if logfilter_return == 0:
            cur.execute("select itemid,value_md5,value,max(ns),count(*) from zm_log_error_filter group by itemid,value_md5,value")
            numrows = int(cur.rowcount)
            for x in range(numrows):
                row = cur.fetchone()
                itemid = row["itemid"]
                value = row["value"]
                value_md5 = "'"+row["value_md5"]+"'"
                max_ns = row["max(ns)"]
                err_count_new = row["count(*)"]
                #print err_count_new,max_ns
                cur1.execute("select err_count,last_time from zm_log_error_groupby where itemid=%s and value_md5=%s"%(itemid,value_md5))
                row1 = cur1.fetchone()
                if row1 is None:
                    #回数据库再去查询最新的完整的报错内容
                    cur2.execute("select value from history_log where itemid=%s and ns=%s"%(itemid,max_ns))
                    row2 = cur2.fetchone()
    
                    hostname,itemname = query_host(itemid)
                    sub = hostname+':'+itemname+'异常日志报警'
                    content = '<br>主机：%s<br> <br>日志监控项：%s<br> <br>报警内容为：%s<br> <br><div style="font-weight: bold;color: red;">该错误为新错误，在过去五分钟已报次数：%s，请处理！</div><br>'%(hostname,itemname,row2["value"],err_count_new)
                    content_wechat = '主机：%s \n日志监控项：%s \n报警内容为：%s \n该错误为新错误，在过去五分钟已报次数：%s，请处理!'%(hostname,itemname,row2["value"],err_count_new)
                    to_list = process_maillist(itemname)
                    #print content
                    send_mail(mail_user, mail_pass, smtp_server, to_list, sub, content)
                    #send_wechat(sub, content_wechat)
                    value_format = "'"+value+"'"
                    mysql_dml("insert into zm_log_error_groupby(itemid,value_md5,value,err_count,last_time) values (%s,%s,%s,%s,%s)"%(itemid,value_md5,value_format,err_count_new,"'"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'"))
                    write_errlog_influxdb('zabbix', 'ks_errorlog_groupby', hostname, err_count_new, row2["value"])

                elif (datetime.datetime.now()-row1["last_time"]).seconds>120*60:
                    mysql_dml("update zm_log_error_groupby set err_count=err_count+%s,last_time=%s where itemid=%s and value_md5=%s"%(err_count_new,"'"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'",itemid,value_md5))
                    #夜间免打扰
                    if int(datetime.datetime.now().strftime("%H")) <6:
                        continue
                    #回数据库再去查询最新的完整的报错内容
                    cur2.execute("select value from history_log where itemid=%s and ns=%s"%(itemid,max_ns))
                    row2 = cur2.fetchone()
    
                    hostname,itemname = query_host(itemid)
                    sub = hostname+':'+itemname+'异常日志报警'
                    content = '<br>主机：%s<br> <br>日志监控项：%s<br> <br>归类的报警内容为：%s<br> <br><div style="font-weight: bold;color: red;">此类错误持续在报，累计次数已达：%s，请处理！</div><br> <br>以下是此类错误中最新的一条完整报错：<br> <br>%s<br>'%(hostname,itemname,value,row1["err_count"]+err_count_new,row2["value"])
                    content_wechat = '主机：%s \n日志监控项：%s \n归类的报警内容为：%s \n此类错误持续在报，累计次数已达：%s，请处理！\n以下是此类错误中最新的一条完整报错：\n %s'%(hostname,itemname,value,row1["err_count"]+err_count_new,row2["value"])
                    to_list = process_maillist(itemname)
                    send_mail(mail_user, mail_pass, smtp_server, to_list, sub, content)
                    #send_wechat(sub, content_wechat)
                    #print content
                    write_errlog_influxdb('zabbix', 'zm_log_error_groupby', hostname, row1["err_count"]+err_count_new, row2["value"])
    
                elif (datetime.datetime.now()-row1["last_time"]).seconds<120*60:
                    mysql_dml("update zm_log_error_groupby set err_count=err_count+%s where itemid=%s and value_md5=%s"%(err_count_new,itemid,value_md5))
        else:
            print "there's no new data in zabbix history_log table,so do nothing!"
    cur2.close()
    cur1.close()
    cur.close()
    conn.close()
except Exception,e:
    print "main process detects error:" ,e
