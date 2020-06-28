'''
Created on 27-Jun-2020

This is implementation a pivoting function using Dataframe.

Pivot Spark DataFrame:
    Spark SQL provides pivot function to rotate the data from
        one column into multiple columns. It is an aggregation where
        one of the grouping columns values transposed into individual
        columns with distinct data. To get the total amount exported to each
        country of each product, will do group by Product, pivot by Country,
        and the sum of Amount.

@author: kasho
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.sql import Row

conf = BaseConfUtils()
sparkContxt = conf.createSparkContext("Pivoting Fun")
sqlContxt = conf.createSQLContext(sparkContxt)
rdd = sparkContxt.parallelize([("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")])
df_data = sqlContxt.createDataFrame(rdd, ["Product","Amount","Country"])
df_data.show()

'''
+-------+------+-------+
|Product|Amount|Country|
+-------+------+-------+
| Banana|  1000|    USA|
|Carrots|  1500|    USA|
|  Beans|  1600|    USA|
| Orange|  2000|    USA|
| Orange|  2000|    USA|
| Banana|   400|  China|
|Carrots|  1200|  China|
|  Beans|  1500|  China|
| Orange|  4000|  China|
| Banana|  2000| Canada|
|Carrots|  2000| Canada|
|  Beans|  2000| Mexico|
+-------+------+-------+

'''

pivotDF = df_data.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.show()

'''
+-------+------+-----+------+----+
|Product|Canada|China|Mexico| USA|
+-------+------+-----+------+----+
| Orange|  null| 4000|  null|4000|
|  Beans|  null| 1500|  2000|1600|
| Banana|  2000|  400|  null|1000|
|Carrots|  2000| 1200|  null|1500|
+-------+------+-----+------+----+
'''
print("Execution Completed")