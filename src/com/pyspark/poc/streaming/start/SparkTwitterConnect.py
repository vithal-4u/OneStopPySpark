'''
Created on 12-Jun-2020

This file will connect with Twitter to read tweets

@author: kasho
'''
import time
import json
from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()

scc = conf.createStreamingContext("Read Twitter Data")

def filter_tweets(tweet):
    print(tweet)
    #json_tweet = json.loads(tweet)
    #print(json_tweet)
    #if json_tweet.has_key('lang'): # When the lang key was not present it caused issues
        #if json_tweet['lang'] == 'ar':
            #return True # filter() requires a Boolean value
    return True

def get_prediction(tweet_text):
    try:
        tweet_text



if __name__ == "__main__":
    IP = "localhost"
    port = 5555
    #scc.checkpoint("checkpoint_TwitterApp")
    lines = scc.socketTextStream(IP,port)
    print("---->",lines)
    #lines.pprint()
    #lines.foreachRDD(lambda rdd : rdd.filter(filter_tweets).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()))




    scc.start()
    scc.awaitTermination()

    #scc.stop()