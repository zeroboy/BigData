# coding=utf-8
import os
from streamparse import Bolt
import json
import redis


class WordCountBolt2(Bolt):
    outputs = ['word']

    def initialize(self, conf, ctx):
        self.pid = os.getpid()
        self.total = 0
        redis_host='0.0.0.0'
        redis_post=6379
        redis_auth='xxxxxx'
        redis_db=1
        self.redis_key="stormdata"
        self.r = redis.StrictRedis(host=redis_host, port=redis_post, password=redis_auth, db=redis_db)

    def process(self, tup):
        tups = tup.values[0]
	self.r.lpush(self.redis_key,tups)

            




