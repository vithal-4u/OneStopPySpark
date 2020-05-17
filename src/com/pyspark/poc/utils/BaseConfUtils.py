'''


'''

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
        conf.setMaster("local").setAppName(appName)
        return conf

    def createSparkContext(self, appName):
        sparkConf = self.createSparkConfig(appName)
        return SparkContext(conf=sparkConf)

    def createSparkSession(self, appName):
        sparkSessObj = SparkSession.builder.master("local").appName(appName).getOrCreate()
        return sparkSessObj

    def createSQLContext(self, appName):
        sparkContext = self.createSparkContext(appName)
        return SQLContext(sparkContext)
