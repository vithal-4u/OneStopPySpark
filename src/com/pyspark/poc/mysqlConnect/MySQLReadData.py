'''
Created on 23-Apr-2020

@author: kasho
'''
from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()
'''
Fetch records using SQLContext.
'''
sparkSess = conf.createSparkSession("Testing")

if __name__ == '__main__':
    # Fetching records using SQLContext
    # source_df = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/sakila").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "actor").option("user", "root").option("password", "ayyappasai").load()
    # source_df.show()

    sourceDF = sparkSess.read \
        .format("jdbc").option("url", "jdbc:mysql://localhost:3306/ONESTOP_SPARK_DB") \
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "Student_Marks") \
        .option("user", "root").option("password", "ayyappasai").load()
    sourceDF.show()

