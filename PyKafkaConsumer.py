#coding=utf-8

import sys
import os
import time
import json
from pykafka import KafkaClient
from pykafka.balancedconsumer import BalancedConsumer
from pykafka.simpleconsumer import OwnedPartition, OffsetType

reload(sys)
sys.setdefaultencoding('utf8')

#curpath = os.path.normpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

#pykafka, need install PyKafka

class PyKafka:

    consumer = None
    TOPIC = 'wcygame'
    BROKER_LIST = 'kafka01:9092,kafka02:9092,kafka03:9092'
    ZK_LIST = 'zk01:2181,zk02:2181,zk03:2181'
    GROUP_ID = 'bolu22'
    ORI_SOURCE = 'D:/BoLuServer/'
    NOW_SOURCE = 'E:/logstash/logstash-5.5.2/logstash-5.5.2/content/'


    server = topic = zsServer = None
    partitions = None

    def __init__(self):
        print("begin pykafka")
        self.server = self.BROKER_LIST
        self.topic = self.TOPIC
        self.zkServer = self.ZK_LIST
        self.groupID = self.GROUP_ID
        self.oriSource = self.ORI_SOURCE
        self.nowSource = self.NOW_SOURCE

    def getConnect(self):
        client = KafkaClient(hosts=self.server)
        topic = client.topics[self.topic]
        self.partitions = topic.partitions.values() #依据topic获取分区
        print 'topic.latest_available_offsets:%s' % topic.latest_available_offsets() #最后一个可获取的偏移量
        #self.consumer = topic.get_balanced_consumer(#集群消费
        #    consumer_group=self.groupID,
        #    auto_offset_reset=OffsetType.EARLIEST,
        #    auto_commit_enable=True,
        #    auto_commit_interval_ms=30*1000,
        #    zookeeper_connect=self.zkServer
        #)

        self.consumer = topic.get_simple_consumer(#单节点消费
         reset_offset_on_start=False,
         consumer_group=self.groupID,
         auto_commit_enable=True,
         auto_commit_interval_ms=1,
        )

        self.consumer.consume() #从消费者那里得到一条消息
        return self.consumer

    def disConnect(self):
        #self.consumer.close()
        pass


    def beginConsumer(self):
        i = 0
        #last = int(self.read_lock())
        #self.consumer.reset_offsets([(last, int(time.time())*1000) for partition in self.partitions])

        for oneLog in self.consumer:
            try:
                dataset = json.loads(oneLog.value)
                #print dataset
                self.write_log(oneLog.offset, dataset)
            except Exception, e:
                print e
            i = i + 1
        #self.consumer.commit_offsets()

    def write_log(self, offset, dt):
        gssid = int(dt['tags'][1])
        path1 = dt['source'].replace('\\', '/')
        path = path1.replace(self.oriSource, self.nowSource)

        onlypaths = path.split("/")
        del(onlypaths[-1])
        onlypath = "/".join(onlypaths)
        #print onlypath
        self.mkdir(onlypath)

        message = dt['message'].strip()
        f = file(path, "a+")
        print message
        f.write(message+"\n")
        f.close()

        return self.write_lock(offset)

    #锁文件 标记消费偏移量
    def write_lock(self, offset):
        try:
            f = file('./%s.lock' % self.topic, "w")
            f.write(str(offset))
            f.close()

            return int(offset)
        except Exception, e:
            print e
            return -1

    def read_lock(self,):
        try:
            f = file('./%s.lock' % self.topic, "r")
            last_Offset = f.read()
            print "last_Offset :%s" % last_Offset
            f.close()
            return int(last_Offset)
        except Exception, e:
            print e
            return -1

    def mkdir(self, path):

        # 去除首位空格
        path = path.strip()
        # 去除尾部 \ 符号
        path = path.rstrip("\\")

        # 判断路径是否存在
        # 存在     True
        # 不存在   False
        isExists = os.path.exists(path)

        # 判断结果
        if not isExists:
            # 如果不存在则创建目录
            # 创建目录操作函数
            os.makedirs(path)
            return True
        else:
            # 如果目录存在则不创建，并提示目录已存在
            return False


if __name__ == '__main__':

    pk = PyKafka()
    pk.getConnect()
    pk.beginConsumer()
