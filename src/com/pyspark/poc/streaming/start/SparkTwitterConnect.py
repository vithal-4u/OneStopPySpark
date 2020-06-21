'''
Created on 12-Jun-2020

This file will read the twitter line from port which we created in TweetRead.py
    after reading each line we are storing it into local file system i.e. window machine

@author: kasho
'''
import time
from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.sql import Row

conf = BaseConfUtils()
sparkContxt = conf.createSparkContext("Twitter Streaming")
ssc = conf.createStreamingContext(sparkContxt)
sqlContxt = conf.createSQLContext(sparkContxt)

def processData(lines):
    words = lines.flatMap(lambda line: line.split("\n"))

    words = words.filter(filterEmptyLines)
    line = words.map(lambda p: Row(name=p))
    df = sqlContxt.createDataFrame(line)
    df.coalesce(1).write.format("text").mode("append").save(
        "D:/Study_Document/pycharm-workspace/PySparkPOC/resources/TwitterRead")


def filterEmptyLines(line):
    if len(line) > 0:
        return  line

if __name__ == "__main__":
    IP = "localhost"
    port = 9009

    socket_stream = ssc.socketTextStream(IP,port)
    lines = socket_stream.window(20)

    lines.pprint()
    lines.foreachRDD(processData)

    ssc.start()
    ssc.awaitTermination()
