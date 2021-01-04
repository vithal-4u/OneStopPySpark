'''
Created on 03-Jan-2021

Basic operation on Window Function

@author: kasho
'''
from bokeh.core.property.container import Seq
from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank
from pyspark.sql.functions import col,avg,sum,min,max,row_number
from src.com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()

sparkSess = conf.createSparkSession("WindowFuncExample")



simpleData = sparkSess.createDataFrame([("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)],["employee_name", "department", "salary"])

simpleData.show()

windowSpec  = Window.partitionBy("department").orderBy("salary")
simpleData.withColumn("row_number", row_number().over(windowSpec)).show()
simpleData.withColumn("rank",rank().over(windowSpec)).show()

##############################
# Aggregate Function
##############################
windowSpecAgg  = Window.partitionBy("department")

simpleData.withColumn("row", row_number().over(windowSpec))\
    .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
    .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
    .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
    .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
    .where(col("row")==1).select("department","avg","sum","min","max") \
    .show()
