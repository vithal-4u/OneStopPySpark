'''
Created on 12-Jun-2020

This file will read the twitter line from port which we created in TweetRead.py
    after reading each line we are storing it into local file system i.e. window machine

@author: kasho
'''
import time
from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()
ssc = conf.createStreamingContext("Twitter Streaming")


if __name__ == "__main__":
    IP = "localhost"
    port = 9009

    socket_stream = ssc.socketTextStream(IP,port)
    lines = socket_stream.window(20)

    lines.pprint()
    lines.saveAsTextFiles("D:/Study_Document/GIT/OneStopPySpark/temp/%f"% time.time())

    ssc.start()
    ssc.awaitTermination()
