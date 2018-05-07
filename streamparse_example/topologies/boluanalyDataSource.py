"""
Word count topology
"""

from streamparse import Grouping, Topology

from bolts.wordcount import WordCountBolt
from bolts.wordcount2 import WordCountBolt2
from bolts.wordcount3 import WordCountBolt3
from spouts.words import WordSpout


class WordCount(Topology):
    #spout1
    word_spout = WordSpout.spec()

    #bolt1
    count_bolt = WordCountBolt.spec(inputs={word_spout: Grouping.fields('word')},par=2) #   process

    #bolt2
    count_bolt2 = WordCountBolt2.spec(inputs={count_bolt: Grouping.fields('word')},par=2)
   
    #bolt3
    count_bolt3 = WordCountBolt3.spec(inputs={count_bolt: Grouping.fields('word')},par=2)


    
