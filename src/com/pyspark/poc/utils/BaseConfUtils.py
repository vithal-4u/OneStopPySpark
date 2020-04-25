'''
Created on 22-Apr-2020
This class is to get basic config related to Spark
@author: kasho
'''
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.context import SQLContext


class BaseConfUntils(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
    
    def createSparkConfig(self, appName):
        conf = SparkConf()
        conf.setMaster("local").setAppName(appName)
        return conf
    
    def createSparkContext(self, appName):
        SparkConf = self.createSparkConfig(appName)
        return SparkContext(conf=SparkConf)
    
    def createSparkSession(self, appName):
        #sparkConf = self.createSparkConfig(appName)
        sparkSessObj = SparkSession.builder.master("local").appName(appName).getOrCreate()
        return sparkSessObj
    
    def createSQLContext(self, appName):
        sparkContxt = self.createSparkContext(appName)
        return SQLContext(sparkContxt)
