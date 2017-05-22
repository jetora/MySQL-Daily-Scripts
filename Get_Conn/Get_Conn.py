#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys
import os
import pymysql
import re, mmap
import datetime
import time

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'


class Get_results():
    def __init__(self, v_str):
        self.host = v_str[0]
        self.port = "3358"
        self.user = "xxx"
        self.passwd = "xxx"
        self.db = "test"

    def get_cursor(self):
        try:
            conn = pymysql.connect(host=self.host, port=int(self.port), user=self.user, passwd=self.passwd, db=self.db,
                                   charset='UTF8')
        except Exception, e:
            print e
        return conn

    def get_conn(self):
        cursor = self.get_cursor().cursor()
        cursor.execute(
            "select count(*) from information_schema.processlist where COMMAND not in ('Binlog Dump') and USER not in ('root','replicater','system user','monitor')")
        results = cursor.fetchone()
        return results


def readContent():
    f = file(r'aa.txt', 'rb')
    content = f.read()
    f.close()
    return content


def getContent():
    content = readContent()
    # print 'content:',repr(content)
    strlist = re.compile(r'(?<![\.\d])(?:\d{1,3}\.){3}\d{1,3}(?![\.\d]) SlaveDelay lag too much')
    iplist = re.compile(r'(?<![\.\d])(?:\d{1,3}\.){3}\d{1,3}(?![\.\d])')
    for value in strlist.findall(content):
        # print iplist.findall(value)
        t_consumer = Get_results(iplist.findall(value))
        print t_consumer.host+": "+str(t_consumer.get_conn()[0])


if __name__ == '__main__':
    getContent()

