#encoding:utf-8
import redis
import time
import json
import pycurl
import StringIO
import urllib
import sys
import certifi


class curlrequest:
    def __init__(self, method, url, data):
        self.method = method
        self.url = url
        self.data = data


    def pull(self):
        b = StringIO.StringIO()
        c = pycurl.Curl()
        c.setopt(pycurl.CAINFO, certifi.where())
        c.setopt(pycurl.URL, self.url)
        c.setopt(pycurl.HTTPHEADER, ["Accept:"])
        c.setopt(pycurl.CUSTOMREQUEST, self.method)
        c.setopt(pycurl.POSTFIELDS, self.data)
        c.setopt(pycurl.WRITEFUNCTION, b.write)
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.MAXREDIRS, 5)
        c.perform()
        datas = b.getvalue()
        status = c.getinfo(c.HTTP_CODE)
        returndata = {"status": status, "data": datas}
        b.close()
        c.close()
        return returndata


class redisexample:
    def __init__(self, host, port, auth, db):
        self.host = host
        self.port = port
        self.auth = auth
        self.db = db
    def instance(self):
        return redis.StrictRedis(host=self.host, port=self.port, password=self.auth, db=self.db)




class datamonitor:
    def __init__(self, data, threshold, rds):
        self.data = data
        self.threshold = threshold
        self.rds = rds.instance()
        self.handle()

    def handle(self):
        self.lists = json.loads(self.data)
        try:
            self.action = self.lists['message'].split(",")[0].split(" ")[-1]
            self.name = self.lists['message'].split(",")[-4].split(" ")[-1]
            self.ip = self.lists['message'].split(",")[-3].split(" ")[-1]
        except Exception,e:
            pass

    #监控
    def monitor(self):
        if self.action.encode("utf8") == "心跳延迟":
            ## 延迟值
            desc = int(self.lists['message'].split(",")[-1].split(" = ")[-1])

            if desc > self.threshold:
                #触发
                self.wraing('delay')
            else:
                #恢复
                self.recovery('delay')


        elif self.action.encode("utf8") in ["连接游戏服超时", "进游戏超时"]:
            # 触发
            self.wraing('timeout')

        elif self.action.encode("utf8") == "连接游戏服成功":
            #恢复
            self.recovery('timeout')



    #报警
    def wraing(self, reason):
        tag_name = "heart_" + self.ip.encode("utf8") + "_" +reason

        if self.rds.get(tag_name):
            heart_info = json.loads(self.rds.get(tag_name).replace("'", '"'))
            send_counts = heart_info["send_counts"]
            waring_counts = heart_info["waring_counts"]
            last_waring_time = heart_info["last_waring_time"]
            now = time.time()

            if waring_counts > 3:
                if now - last_waring_time > send_counts*600:
                    police_content = ""
                    police_content += "host: " + self.lists["beat"]["hostname"] + "\n"
                    police_content += "message: " + self.lists['message']
                    request_url = 'https://api.xxx.com/police/police_api'
                    request_data = "police_id=2&"
                    request_data += "police_content=" + urllib.quote(police_content.encode('utf8'))
                    cq = curlrequest('POST', request_url, request_data)
                    print cq.pull()
                    #update
                    last_waring_time = now
                    send_counts += 1

            save_content = {
                "send_counts": send_counts,
                "waring_counts": waring_counts + 1,
                "last_waring_time": last_waring_time
            }

            self.rds.set(tag_name, save_content)

        else:
            #初始化
            save_content = {
                "send_counts": 0,
                "waring_counts": 1,
                "last_waring_time": 0
            }
            self.rds.set(tag_name, save_content)

    #恢复
    def recovery(self, reason):
        tag_name = "heart_" + self.ip.encode("utf8") + "_" + reason
        total_name = tag_name + "_total"
        hash_name = 'record_heartbeat'
        if self.rds.get(tag_name):
            heart_info = json.loads(self.rds.get(tag_name).replace("'", '"'))
            send_counts = heart_info["send_counts"]
            waring_counts = int(heart_info["waring_counts"])

            #恢复推送
            if send_counts > 0:
                police_content = ""
                police_content += "原因: "+reason+"，IP：" + self.ip.encode("utf8") + "，状态: 恢复正常" + "\n"
                police_content += "到恢复发送推送次数: " + str(send_counts) + "\n"
                police_content += "到恢复前报警次数: " + str(waring_counts) + "\n"

                request_url = 'https://api.xxx.com/police/police_api'
                request_data = "police_id=2&"
                request_data += "police_content=" + urllib.quote(police_content)
                cq = curlrequest('POST', request_url, request_data)
                print cq.pull()

            #合计记录
            record = self.rds.hget(name=hash_name, key=total_name)
            if record != None:
                record_n = int(record) + waring_counts
                self.rds.hset(name=hash_name, key=total_name, value=record_n)
            else:
                self.rds.hset(name=hash_name, key=total_name, value=waring_counts)

            #记录复原
            self.rds.delete(tag_name)





def main():
    rdse = redisexample('0.0.0.0', 6379, 'Redis2017', 1)
    rds = rdse.instance()
    threshold = 100
    while True:
        row = rds.lpop("heartbeatwarning")
        if row != None:
            dm = datamonitor(row, threshold, rdse)
            dm.monitor()
        else:
            time.sleep(2)




if __name__ == '__main__':
    main()




