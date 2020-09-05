'''
Created on 05-Sept-2020

    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995

    Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
    take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file. Keep in mind, that the original
    log files contains the following header lines.
    host    logname    time    method    url    response    bytes
    Make sure the head lines are removed in the resulting RDD.

'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils


conf = BaseConfUtils()
sc = conf.createSparkContext("UnionSampleLog")

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)

if __name__ == "__main__":
    julyFirstLogs = sc.textFile("D:/Study_Document/GIT/OneStopPySpark/resources/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("D:/Study_Document/GIT/OneStopPySpark/resources/nasa_19950801.tsv")

    aggregatedLogs = julyFirstLogs.union(augustFirstLogs)
    cleanLogHeader = aggregatedLogs.filter(isNotHeader)
    sampleRDD = cleanLogHeader.sample(withReplacement= True, fraction=0.1)
    sampleRDD.saveAsTextFile("D:/Study_Document/GIT/OneStopPySpark/out/sample_nasa_logs.csv")
    
    print("Execution Completed")