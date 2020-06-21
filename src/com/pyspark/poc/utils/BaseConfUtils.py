'''


'''
from pyspark.streaming import StreamingContext

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.context import SQLContext

class BaseConfUtils(object):
    '''

    '''

    def __init__(self):
        '''
        constructor
        '''


    def createSparkConfig(self,appName):
        conf = SparkConf()
        conf.set("spark.driver.allowMultipleContexts", "true").setMaster("local[2]").setAppName(appName)
        return conf

    def createSparkContext(self, appName):
        sparkConf = self.createSparkConfig(appName)
        return SparkContext(conf=sparkConf).getOrCreate()

    def createSparkSession(self, appName):
        sparkSessObj = SparkSession.builder.master("local[2]").appName(appName).getOrCreate()
        return sparkSessObj

    def createSQLContext(self,sparkContext):
        return SQLContext(sparkContext)

    def createStreamingContext(self, sparkContext):
        return StreamingContext(sparkContext, 10)