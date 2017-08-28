#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys
import os
import MySQLdb
import codecs
import multiprocessing
import paramiko
from optparse import OptionParser
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'

def get_cli_options():
        parser = OptionParser(usage="usage: python %prog [options]",
                                  description=""".........table_open_cache.........""")
        (options, args) = parser.parse_args()
        return options
class Master():
    def __init__(self,v_str):
        self.host=v_str.strip()
        self.port=xxx
        self.user="xxx"
        self.passwd="xxx"
        self.db="test"
        
    def get_cursor(self):       
        try:
            conn = MySQLdb.connect(host=self.host, port=int(self.port), user=self.user, passwd=self.passwd, db=self.db, charset='UTF8')
        except Exception , e:
            print e
        return conn 


def get_sub():
    
    options = get_cli_options() 
    t_sql = 'set global table_open_cache=45535;'
    #result1=c_master.fetchall()
    file = codecs.open('iplist2openfile.txt', 'rb', 'utf-8')
    NumList = []
    cmd = "sed -i 's/table_open_cache[ \t]*= 16384/table_open_cache\t\t= 35535/g' /export/servers/mysql/etc/my.cnf"
    for Line in file:
        NumList.append(Line)
    #print NumList
    
    for i in range (len(NumList)):
        print NumList[i]
        t_master = Master(NumList[i])
        c_master=t_master.get_cursor().cursor()
        c_master.execute(t_sql)        
        exec_cmd(NumList[i],cmd)
    
def exec_cmd(host,cmd):
    try:
        username = "root"
        passwd = "byfLMurtD8E7O4xNmdznCsoYS3UJIq"
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, port=22, username=username, password=passwd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.readlines()
        ssh.close()
        return result
    except Exception, e:
        print e
        return 0

if __name__ == '__main__':
    get_sub()

