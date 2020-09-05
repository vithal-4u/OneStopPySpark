'''
Created on 05-Sept-2020

    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....
    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes
    Make sure the head lines are removed in the resulting RDD.

'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils


conf = BaseConfUtils()
sc = conf.createSparkContext("SameHostsIntersection")

if __name__ == "__main__":
    julyFirstLogs = sc.textFile("D:/Study_Document/GIT/OneStopPySpark/resources/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("D:/Study_Document/GIT/OneStopPySpark/resources/nasa_19950801.tsv")

    julyFirstHost = julyFirstLogs.map(lambda line: line.split("\t")[0])
    augustFirstHost = augustFirstLogs.map(lambda line: line.split("\t")[0])

    intersectionData = julyFirstHost.intersection(augustFirstHost)
    cleanHostInter = intersectionData.filter(lambda host: host != "host")
    cleanHostInter.saveAsTextFile("D:/Study_Document/GIT/OneStopPySpark/out/nasa_logs_same_hosts.csv")

    print("Execution Completed")