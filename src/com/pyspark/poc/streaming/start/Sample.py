'''
Created on 12-Jun-2020

This file will connect with Twitter to read tweets

@author: kasho
'''
import time
import json

from pyspark import StorageLevel

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()

scc = conf.createStreamingContext("Read Twitter Data")

def dataGot(data):
    print(data)

if __name__ == "__main__":
    IP = "localhost"
    port = 9009
    scc.checkpoint("checkpoint_TwitterApp")
    lines = scc.socketTextStream(IP,port)

    #words = lines.flatMap(lambda line: line.split(" "))

    #pairs = words.map(lambda word: (word, 1))

    lines.foreachRDD(dataGot)
    #wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this DStream to the console
    #wordCounts.pprint()
    #wordCounts.foreachRDD(dataGot)

    scc.start()
    scc.awaitTermination()