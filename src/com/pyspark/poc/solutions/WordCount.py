from src.com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

if __name__ == "__main__":
    config = BaseConfUtils()
    # create Spark context with necessary configuration
    sc = config.createSparkContext("PySpark Word Count Exmaple")
    #conf = SparkConf()
    #conf.setMaster("local").setAppName("WordCount")
    #sc = SparkContext(conf=conf)

    # read data from text file and split each line into words
    words = sc.textFile("D:/Study_Document/pycharm-workspace/PySparkPOC/resources/wordCount.txt").flatMap(
        lambda line: line.split(" "))

    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    output = wordCounts.collect()

    for (word, count) in output:
        print("%s: %i" % (word, count))
    print("Execution Completed")
