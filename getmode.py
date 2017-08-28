#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys
import os
import json
import MySQLdb
import socket
import fcntl
import struct
import multiprocessing
import subprocess
from optparse import OptionParser
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'

def get_cli_options():
        parser = OptionParser(usage="usage: python %prog [options]",
                                  description=""".........get read mode.........""")
        parser.add_option("-d",dest="ip",
                              help="IP",default='127.0.0.1')
        parser.add_option("-t",dest="type",help="item name")
        (options, args) = parser.parse_args()
        return options

class Mydb():
    def __init__(self,v_str):
        self.host=v_str.strip()
        self.port=xxx
        self.user="xxx"
        self.passwd="xxx"
        #self.db="test"
        
    def get_data(self,t_sql):       
        try:
            #conn = MySQLdb.connect(host=self.host, port=int(self.port), user=self.user, passwd=self.passwd, db=self.db, charset='UTF8')
            conn = MySQLdb.connect(host=self.host, port=int(self.port), user=self.user, passwd=self.passwd, charset='UTF8')
        except Exception , e:
            print 'Can Not Connect to Mysql...'
            print e
        c=conn.cursor()
        c.execute(t_sql)
        result=c.fetchall()
        return result

def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

def get_sub():
    t_sql1 = "show global variables like 'read_only'"
    t_sql2 = "show slave status"
    cmd1 = "cat /export/servers/mysql/etc/my.cnf |grep read_only|awk -F '=' '{print $2}'"
    cmd2 = "touch /export/data/mysql/dumps/aa"
    cmd3 = "ifconfig | grep 'inet addr:' | grep -v '127.0.0.1' | cut -d: -f2 | awk '{print $1}' | head -1"
    options = get_cli_options() 
    mydb = Mydb(options.ip)
    sessmode = mydb.get_data(t_sql1)[0][1]
    if sessmode == "OFF":
        sessmode = 0
    else:
        sessmode = 1
    
    sla_arr = mydb.get_data(t_sql2)
    if (len(sla_arr) == 0): 
        print len(sla_arr)
        #is_master = 'Y'
        is_master = 1
    elif sla_arr[0][10] == "No" and sla_arr[0][11] == "No":
        is_master = 1
        #is_master = 'Y'
    else:
        is_master = 0
        #is_master = 'N'

    retcode = subprocess.call(cmd2,shell=True)
    if retcode == 0:
        #filemode='OFF'
        filemode=0
    elif retcode == 1:
        #filemode='ON'
        filemode=1

    #p = subprocess.Popen(cmd1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #t_sysmode = p.stdout.readlines()[0].strip()
    t_sysmode = os.popen(cmd1).read().strip() 
    if t_sysmode == '0':
        #sysmode = 'OFF'
        sysmode = 0
    else:
        #sysmode = 'ON'
        sysmode = 1
    #for line in p.stdout.readlines():
    #    print line, 
    #retval = p.wait()

    t_ip = os.popen(cmd3).read().strip() 
    

    #data = { 'ip' : get_ip_address('eth0'), 'ismaster' : is_master, 'sessmode' : sessmode, 'sysmode' : sysmode, 'filemode' : filemode }
    data = { 'ip' : t_ip, 'ismaster' : is_master, 'sessmode' : sessmode, 'sysmode' : sysmode, 'filemode' : filemode }
    #jsond = json.dumps(data,indent=4)
    #return jsond
    return data[options.type]

if __name__ == '__main__':
    print get_sub()

