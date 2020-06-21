'''
Created on 12-Jun-2020

This file will connect with Twitter to read tweets

@author: kasho
'''
import time

from pyspark.sql import Row

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.conf import SparkConf

conf = BaseConfUtils()
sc = conf.createSparkContext("PySpark Word Count Exmaple")
sqlContxt = conf.createSQLContext(sc)

def filterEmptyLines(line):
    if len(line) > 0:
        return  line


if __name__ == "__main__":


    words = sc.textFile("D:/Study_Document/pycharm-workspace/PySparkPOC/resources/wordCount.txt").flatMap(
        lambda line: line.split("\n"))
    words = words.filter(filterEmptyLines)
    line = words.map(lambda p: Row(name=p))
    df = sqlContxt.createDataFrame(line)
    #output = line.collect()
    df.coalesce(1).write.format("text").mode("append").save("D:/Study_Document/pycharm-workspace/PySparkPOC/resources/WordCount1.txt")
    df.show()
    # for (word) in output:
    #     print(word)

    print("Execution Completed")
