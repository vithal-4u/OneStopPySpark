'''
Created on 09-Jun-2020

Basic operation on DataFrames

@author: kasho
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()
sparkSess = conf.createSparkSession("DataFrameBasics")

df = sparkSess.read.json("D:/Study_Document/GIT/OneStopPySpark/resources/people.json")
df.show()
'''
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+

'''

df.printSchema()
'''
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
'''

## Creating new columns
df.withColumn('newage',df['age']).show()

'''
+----+-------+------+
| age|   name|newage|
+----+-------+------+
|null|Michael|  null|
|  30|   Andy|    30|
|  19| Justin|    19|
+----+-------+------+
'''

## Simple Rename
df.withColumnRenamed('age','supernewage').show()
'''
+-----------+-------+
|supernewage|   name|
+-----------+-------+
|       null|Michael|
|         30|   Andy|
|         19| Justin|
+-----------+-------+
'''

df.withColumn('doubleage',df['age']*2).show()
'''
+----+-------+---------+
| age|   name|doubleage|
+----+-------+---------+
|null|Michael|     null|
|  30|   Andy|       60|
|  19| Justin|       38|
+----+-------+---------+

INFO : Same way we can do add, subtract and divide for each value of the column as above.
'''

###
# To do SQL operation on Created DataFrame, we need register it to a temporary View
#
###
## Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

sql_results = sparkSess.sql("SELECT * FROM people")
print(sql_results)
sql_results.show()
'''
DataFrame[age: bigint, name: string]
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
'''

sparkSess.sql("SELECT * FROM people WHERE age=30").show()
'''
+---+----+
|age|name|
+---+----+
| 30|Andy|
+---+----+
'''