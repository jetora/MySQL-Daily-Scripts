#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys
import os
import MySQLdb
import codecs
import multiprocessing
from optparse import OptionParser
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'

def get_cli_options():
        parser = OptionParser(usage="usage: python %prog [options]",
                                  description=""".........export txt.........""")
        parser.add_option("-t",dest="type",
                              help="Usage:  1)set ,2)recover")
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
    if options.type=='1':
        t_sql = 'Set global sync_binlog=0;Set global innodb_flush_log_at_trx_commit=0;'
    elif options.type=='2':
        t_sql = 'Set global sync_binlog=1;Set global innodb_flush_log_at_trx_commit=1;'
    
    #result1=c_master.fetchall()
    file = codecs.open('iplist.txt', 'rb', 'utf-8')
    NumList = []
    for Line in file:
        NumList.append(Line)
    #print NumList
    
    for i in range (len(NumList)):
        print NumList[i]
        t_master = Master(NumList[i])
        c_master=t_master.get_cursor().cursor()
        c_master.execute(t_sql)        
    

if __name__ == '__main__':
    get_sub()

