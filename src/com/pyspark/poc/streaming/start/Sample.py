'''
Created on 12-Jun-2020

This file will connect with Twitter to read tweets

@author: kasho
'''
import time

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()
ssc = conf.createStreamingContext("Twitter Streaming")

def filter(rdd):
    rdd = rdd.filter(lambda word: word.lower().startswith("#"))
if __name__ == "__main__":
    IP = "localhost"
    port = 9009

    socket_stream = ssc.socketTextStream(IP,port)
    lines = socket_stream.window(20)
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda x: (x, 1)) \
    #     .reduceByKey(lambda a, b: a + b)
    # counts.pprint()
    lines.pprint()
    lines.saveAsTextFiles("D:/Study_Document/GIT/OneStopPySpark/temp/%f"% time.time())
    # counts = lines.flatMap(lambda line: line.split("\n"))
    # counts.pprint()
    ssc.start()
    ssc.awaitTermination()
