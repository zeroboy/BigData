# coding=utf8
import sys
import os
import time
from elasticsearch import Elasticsearch

#es根据唯一字段删除崩溃产生的重复数据


class DataRepeatDel:
    def __init__(self, field, stime, etime):
        self.index = "gamelog-20104-boluanaly"
        self.es_url = "http://10.0.0.232:9200"
        self.es = Elasticsearch(self.es_url)
        self.field = field
        self.aggname = self.field+"_get"
        self.stime = int(time.mktime(time.strptime(stime, '%Y/%m/%d')))
        self.etime = int(time.mktime(time.strptime(etime, '%Y/%m/%d')))

        self.FIELDBUCKETTEMP = '''
                {
                "query": {
                    "bool": {
                        "filter": {
                            "range": {
                                "ctime": {
                                    "gte": %(stime)d,
                                    "lte": %(etime)d
                                }
                            }
                        }
                    }
                },
                "aggs": {
                    "%(aggname)s": {
                        "terms": {
                            "field": "%(field)s",
                            "size": 10000
                        }
                    }
                },
                "size": 0
                }
        '''

        self.BUCKETROWTEMP = '''
                {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"%(filed)s": "%(value)s"}}
                        ]
                    }
                },
                "size": 100
                }
        '''

    def _handel(self):
        body = self.FIELDBUCKETTEMP % {
            "stime": self.stime,
            "etime": self.etime,
            "aggname": self.aggname,
            "field": self.field+".keyword"
        }
        res = self.es.search(index=self.index, body=body)

        times = 1
        for row in res['aggregations'][self.aggname]['buckets']:
            if int(row['doc_count']) > 1:
                print times
                body2 = self.BUCKETROWTEMP % {
                    "field": self.field,
                    "value": row['key']
                }
                res = self.es.search(index=self.index, body=body2)

                print len(res['hits']['hits'])
                for i in range(1, len(res['hits']['hits'])):
                    id = res['hits']['hits'][i]['_id']
                    field = res['hits']['hits'][i]['_source'][self.field]
                    doc_type = res['hits']['hits'][i]['_type']
                    print id, field, doc_type
                    #self.es.delete(index=self.index, doc_type=doc_type, id=id)
                    print '------------------' + id + '------------------del------------------'

                times = times + 1
            else:
                print row


if __name__ == "__main__":
    field = "LogID"
    stime = "2018/05/06"
    etime = "2018/05/07"
    dr = DataRepeatDel(field, stime, etime)
    dr._handel()










