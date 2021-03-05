'''
Created on 02-Mar-2021
    This class will read data from AWS s3

@author: kasho
'''
from src.com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
import os
import pyspark

# This code is failing due to download issue
#os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.common:commons-math3:3.1.1,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:3.1.1 pyspark-shell"

conf = BaseConfUtils()
sc = conf.createSparkContext("Read File from S3")
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.access.key", "<Access-key>")
hadoop_conf.set("fs.s3a.secret.key", "<secret-value>")
hadoop_conf.set("fs.s3a.endpoint", "ap-south-1.amazonaws.com")

sql= pyspark.sql.SparkSession(sc)
dataS3=sql.read.text("s3a://data-storage-v1/airports.txt")
dataS3.show(truncate=False)
