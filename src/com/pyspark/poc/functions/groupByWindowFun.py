'''
Created on 09-Jun-2020

This Script is to read data from csv file and group the highest salary and find/display the highest
    department salary for the location

@author: kasho
'''
from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()
sparkSess = conf.createSparkSession("groupByWindowFun")
empDF = sparkSess.read.csv("D:/Study_Document/GIT/OneStopPySpark/resources/EmpRandom.csv",header=True)
empDF.show()
empDF.printSchema()

dfGrp = empDF.select(empDF["Eid"],empDF["Location"],
                     empDF["Dep"],empDF["Salary"]).groupBy("Location","Dep").agg({'Salary':'sum'})

dfGrp.show()

'''
Output of dfGrp:
+--------+---+-----------+
|Location|Dep|sum(Salary)|
+--------+---+-----------+
|     Ban| IT|   124000.0|
|     Hyd| IT|    60000.0|
|     PUN| IT|   170000.0|
|     Hyd|  A|    25000.0|
|     Ban|  A|    60000.0|
|     PUN|  A|   143000.0|
+--------+---+-----------+

'''

from pyspark.sql import Window
import pyspark.sql.functions as f

w = Window.partitionBy('Location')
dfGrp.withColumn('maxSal', f.max('sum(Salary)').over(w))\
    .where(f.col('sum(Salary)') == f.col('maxSal'))\
    .drop('maxSal')\
    .show()

'''
Window operation applied with output:
+--------+---+-----------+
|Location|Dep|sum(Salary)|
+--------+---+-----------+
|     PUN| IT|   170000.0|
|     Hyd|  A|    75000.0|
|     Ban| IT|   124000.0|
+--------+---+-----------+
'''
print("Execution Completed")