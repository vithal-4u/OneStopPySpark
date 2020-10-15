'''
Created on 18-July-2020
Requirement:
    Read Json and List out the count of #TAG word with user name of it.

@author: Ashok Kumar
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.sql.functions import explode

conf = BaseConfUtils()
spark = conf.createSparkSession("HashTagCount")

def iteration(user):
    print(user)

def fetchRowData(row):
    user_name=row["USER_NAME"]
    text=row["TEXT"]
    words=text.split(" ")
    return (user_name,words)

def filterMyHashTag(row):
    l = list()
    key=row[0]
    for word in row[1]:
        if "#" in word:
            l.append(word)

    rowval=(key,l)
    return rowval
if __name__ == "__main__":
    #userTweets = spark.read.json("D:/Study_Document/GIT/OneStopPySpark/resources/Epam.json").rdd
    #userTweets.foreach(iteration)
    #userTweets.flatMap(lambda x: iteration(x))

    userTweets = spark.read.json("D:/Study_Document/GIT/OneStopPySpark/resources/Epam.json")
    userTweets.printSchema()
    #nobelRDD = userTweets.select('USER_NAME', explode(userTweets['TEXT'])).rdd
    #nobelRDD.collect()
    userTweetsPojo = userTweets.createTempView("UserTweets")
    userTextRDD = spark.sql("SELECT USER_NAME,TEXT FROM UserTweets").rdd
    userSplitTextRDD=userTextRDD.map(lambda row : fetchRowData(row))
    userHashTagTextRDD=userSplitTextRDD.flatMap(lambda row: filterMyHashTag(row))

    #fitlerVal=key.filter(lambda row : filterMyHashTag(row))
    userHashTagTextRDD.foreach(print)
    userHashTagTextRDD.createDataFrame()
    df = userHashTagTextRDD.toDF()
    print(df)
    #print(fitlerVal.take(10))
    #print(teenagerNamesDF.take(10))
    #print(teenagerNamesDF['USER_NAME'])
    #teenagerNamesDF.show()TSAP08hc513.


