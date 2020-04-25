'''
Created on 20-Apr-2020

@author: kasho
'''

from pyspark import SparkContext, SparkConf
from com.pyspark.poc.utils import BaseConfUtils
from com.pyspark.poc.utils.BaseConfUtils import BaseConfUntils

if __name__ == "__main__":
    config = BaseConfUntils()
    # create Spark context with necessary configuration
    sc = config.createSparkContext("PySpark Word Count Exmaple")

    # read data from text file and split each line into words
    words = sc.textFile("D:/Study_Document/pycharm-workspace/OneStopPySpark/resources/wordcountData.txt").flatMap(lambda line: line.split(" "))
    
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    output = wordCounts.collect()

    for (word, count) in output:
        print("%s: %i" % (word, count))
    print("Execution Completed")