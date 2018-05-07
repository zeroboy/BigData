# coding=utf-8
import os
import sys
from streamparse import Bolt
import json


class WordCountBolt(Bolt):
    outputs = ['word']

    def initialize(self, conf, ctx):
        self.pid = os.getpid()
        self.total = 0

    def process(self, tup):
        try:
            tups = tup.values[0]
            kafkadata = json.loads(tups)
            if "BoluAnaly" in kafkadata["source"]:
                word = kafkadata["message"]
                ##
                nstr = word.replace('  ', ' ')
                alldata = nstr.split(', ')
                if len(alldata) <= 1: sys.exit(0)
                data1 = alldata[0]
                data2 = alldata[1]
                date = " ".join((data1.split(" ")[0], data1.split(" ")[1]))
                logtype = data1.split(" ")[2]
                Logid = data2.split(" = ")[1]
                alldata.remove(alldata[0])
                alldata.remove(alldata[0])
                message = {}
                for row in alldata:
                    rows = row.split(" = ")
                    # print rows
                    if len(rows) > 1:
                        if rows[1].isdigit():
                            vaule = int(rows[1])
                        else:
                            vaule = rows[1]

                        message[rows[0]] = vaule
                try:
                    # words = date+"|"+logtype+"|"+Logid
                    words = json.dumps((date, logtype, Logid, message))
                    self.logger.info(words)
                    self.logger.info(word)
                    self.emit([words])
                except Exception, e:
                    self.logger.info(e)
        except Exception, e:
            self.logger.info(e)
            sys.exit(0)

