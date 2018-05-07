from itertools import cycle
from pykafka import KafkaClient
from streamparse import Spout


class WordSpout(Spout):
    outputs = ['word']

    def initialize(self, stormconf, context):
        self.server = 'kafka01:9092,kafka02:9092,kafka03:9092'
        self.topicid = 'xxxxxx'
        self.groupID = 'stormSpouts'
        client = KafkaClient(hosts=self.server)
        topic = client.topics[self.topicid]
        self.consumer = topic.get_simple_consumer(
            reset_offset_on_start=False,
            consumer_group=self.groupID,
            auto_commit_enable=True,
            auto_commit_interval_ms=1,
        )


    def next_tuple(self):
        word = str(self.consumer.consume().value)
        self.emit([word])
