'''
Created on 16-Jun-2020

This file will stream folder and will read the newly generated files using PySpark Streaming.

@author: kasho
'''


from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()
sparkContxt = conf.createSparkContext("Streaming Local File System")
scc = conf.createStreamingContext(sparkContxt)


if __name__ == "__main__":
    lines = scc.textFileStream("D:/Study_Document/GIT/OneStopPySpark/temp/")  # 'log/ mean directory name
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()
    scc.start()
    scc.awaitTermination()