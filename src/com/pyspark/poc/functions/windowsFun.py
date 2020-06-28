'''
Created on 27-Jun-2020

Window Functions or Windowed Aggregates:
Window (also, windowing or windowed) functions perform a calculation over a set of rows.
    It is an important tool to do statistics. Most Databases support Window functions.
    Spark from version 1.4 start supporting Window functions. Spark Window Functions have the following traits:
        a. perform a calculation over a group of rows, called the Frame.
        b. a frame corresponding to the current row
        c. return a new value to for each row by an aggregate/window function
        d. Can use SQL grammar or DataFrame API.

@author: kasho
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

conf = BaseConfUtils()
sparkContxt = conf.createSparkContext("Windows Fun")
sqlContxt = conf.createSQLContext(sparkContxt)

@udf("long")
def median_udf(s):
  index = int(len(s) / 2)
  return s[index]

if __name__ == "__main__":
    empsalary_data = [
      ("sales",     1,  "Alice",  5000, ["game",  "ski"]),
      ("personnel", 2,  "Olivia", 3900, ["game",  "ski"]),
      ("sales",     3,  "Ella",   4800, ["skate", "ski"]),
      ("sales",     4,  "Ebba",   4800, ["game",  "ski"]),
      ("personnel", 5,  "Lilly",  3500, ["climb", "ski"]),
      ("develop",   7,  "Astrid", 4200, ["game",  "ski"]),
      ("develop",   8,  "Saga",   6000, ["kajak", "ski"]),
      ("develop",   9,  "Freja",  4500, ["game",  "kajak"]),
      ("develop",   10, "Wilma",  5200, ["game",  "ski"]),
      ("develop",   11, "Maja",   5200, ["game",  "farming"])]

    empsalary = sqlContxt.createDataFrame(empsalary_data,
        schema=["depName", "empNo", "name", "salary", "hobby"])

    empsalary.show()

    '''
    +---------+-----+------+------+---------------+
    |  depName|empNo|  name|salary|          hobby|
    +---------+-----+------+------+---------------+
    |    sales|    1| Alice|  5000|    [game, ski]|
    |personnel|    2|Olivia|  3900|    [game, ski]|
    |    sales|    3|  Ella|  4800|   [skate, ski]|
    |    sales|    4|  Ebba|  4800|    [game, ski]|
    |personnel|    5| Lilly|  3500|   [climb, ski]|
    |  develop|    7|Astrid|  4200|    [game, ski]|
    |  develop|    8|  Saga|  6000|   [kajak, ski]|
    |  develop|    9| Freja|  4500|  [game, kajak]|
    |  develop|   10| Wilma|  5200|    [game, ski]|
    |  develop|   11|  Maja|  5200|[game, farming]|
    +---------+-----+------+------+---------------+

    '''

    overCategory = Window.partitionBy("depName")
    df = empsalary.withColumn(
        "salaries", collect_list("salary").over(overCategory)).withColumn(
        "average_salary", (avg("salary").over(overCategory)).cast("int")).withColumn(
        "total_salary", sum("salary").over(overCategory)).select(
        "depName", "empNo", "name", "salary", "salaries", "average_salary", "total_salary")
    df.show(20, False)

    '''
    +---------+-----+------+------+------------------------------+--------------+------------+
    |depName  |empNo|name  |salary|salaries                      |average_salary|total_salary|
    +---------+-----+------+------+------------------------------+--------------+------------+
    |develop  |7    |Astrid|4200  |[4200, 6000, 4500, 5200, 5200]|5020          |25100       |
    |develop  |8    |Saga  |6000  |[4200, 6000, 4500, 5200, 5200]|5020          |25100       |
    |develop  |9    |Freja |4500  |[4200, 6000, 4500, 5200, 5200]|5020          |25100       |
    |develop  |10   |Wilma |5200  |[4200, 6000, 4500, 5200, 5200]|5020          |25100       |
    |develop  |11   |Maja  |5200  |[4200, 6000, 4500, 5200, 5200]|5020          |25100       |
    |sales    |1    |Alice |5000  |[5000, 4800, 4800]            |4866          |14600       |
    |sales    |3    |Ella  |4800  |[5000, 4800, 4800]            |4866          |14600       |
    |sales    |4    |Ebba  |4800  |[5000, 4800, 4800]            |4866          |14600       |
    |personnel|2    |Olivia|3900  |[3900, 3500]                  |3700          |7400        |
    |personnel|5    |Lilly |3500  |[3900, 3500]                  |3700          |7400        |
    +---------+-----+------+------+------------------------------+--------------+------------+

    '''

    #####################################################################
    # Ordered Frame with partitionBy and orderBy
    #####################################################################
    overCategory = Window.partitionBy("depName").orderBy(desc("salary"))
    df = empsalary.withColumn(
        "salaries", collect_list("salary").over(overCategory)).withColumn(
        "average_salary", (avg("salary").over(overCategory)).cast("int")).withColumn(
        "total_salary", sum("salary").over(overCategory)).select(
        "depName", "empNo", "name", "salary", "salaries", "average_salary", "total_salary")
    df.show(20, False)

    '''
    +---------+-----+------+------+------------------------------+--------------+------------+
    |depName  |empNo|name  |salary|salaries                      |average_salary|total_salary|
    +---------+-----+------+------+------------------------------+--------------+------------+
    |develop  |8    |Saga  |6000  |[6000]                        |6000          |6000        |
    |develop  |10   |Wilma |5200  |[6000, 5200, 5200]            |5466          |16400       |
    |develop  |11   |Maja  |5200  |[6000, 5200, 5200]            |5466          |16400       |
    |develop  |9    |Freja |4500  |[6000, 5200, 5200, 4500]      |5225          |20900       |
    |develop  |7    |Astrid|4200  |[6000, 5200, 5200, 4500, 4200]|5020          |25100       |
    |sales    |1    |Alice |5000  |[5000]                        |5000          |5000        |
    |sales    |3    |Ella  |4800  |[5000, 4800, 4800]            |4866          |14600       |
    |sales    |4    |Ebba  |4800  |[5000, 4800, 4800]            |4866          |14600       |
    |personnel|2    |Olivia|3900  |[3900]                        |3900          |3900        |
    |personnel|5    |Lilly |3500  |[3900, 3500]                  |3700          |7400        |
    +---------+-----+------+------+------------------------------+--------------+------------+
    '''

    #####################################################################
    # Rank functions in a group
    #####################################################################
    overCategory = Window.partitionBy("depName").orderBy(desc("salary"))
    df = empsalary.withColumn(
        "salaries", collect_list("salary").over(overCategory)).withColumn(
        "rank", rank().over(overCategory)).withColumn(
        "dense_rank", dense_rank().over(overCategory)).withColumn(
        "row_number", row_number().over(overCategory)).withColumn(
        "ntile", ntile(3).over(overCategory)).withColumn(
        "percent_rank", percent_rank().over(overCategory)).select(
        "depName", "empNo", "name", "salary", "rank", "dense_rank", "row_number", "ntile", "percent_rank")
    df.show(20, False)

    '''
    +---------+-----+------+------+----+----------+----------+-----+------------+
    |depName  |empNo|name  |salary|rank|dense_rank|row_number|ntile|percent_rank|
    +---------+-----+------+------+----+----------+----------+-----+------------+
    |develop  |8    |Saga  |6000  |1   |1         |1         |1    |0.0         |
    |develop  |10   |Wilma |5200  |2   |2         |2         |1    |0.25        |
    |develop  |11   |Maja  |5200  |2   |2         |3         |2    |0.25        |
    |develop  |9    |Freja |4500  |4   |3         |4         |2    |0.75        |
    |develop  |7    |Astrid|4200  |5   |4         |5         |3    |1.0         |
    |sales    |1    |Alice |5000  |1   |1         |1         |1    |0.0         |
    |sales    |3    |Ella  |4800  |2   |2         |2         |2    |0.5         |
    |sales    |4    |Ebba  |4800  |2   |2         |3         |3    |0.5         |
    |personnel|2    |Olivia|3900  |1   |1         |1         |1    |0.0         |
    |personnel|5    |Lilly |3500  |2   |2         |2         |2    |1.0         |
    +---------+-----+------+------+----+----------+----------+-----+------------+
    '''

    overCategory = Window.partitionBy("depName").orderBy(desc("salary"))
    df = empsalary.withColumn(
        "row_number", row_number().over(overCategory)).filter(
        "row_number <= 2").select(
        "depName", "empNo", "name", "salary")
    df.show(20, False)
    '''
    +---------+-----+------+------+
    |depName  |empNo|name  |salary|
    +---------+-----+------+------+
    |develop  |8    |Saga  |6000  |
    |develop  |10   |Wilma |5200  |
    |sales    |1    |Alice |5000  |
    |sales    |3    |Ella  |4800  |
    |personnel|2    |Olivia|3900  |
    |personnel|5    |Lilly |3500  |
    +---------+-----+------+------+
    '''
    #####################################################################
    # Lag & Lead in a group
    # lag and lead can be used, when we want to get a relative result between rows.
    #   The real values we get are depending on the order.
    #
    # lag means getting the value from the previous row; lead means getting the value from the next row.
    #####################################################################
    overCategory = Window.partitionBy("depname").orderBy(desc("salary"))
    df = empsalary.withColumn(
        "lead", lead("salary", 1).over(overCategory)).withColumn(
        "lag", lag("salary", 1).over(overCategory)).select(
        "depName", "empNo", "name", "salary", "lead", "lag")
    df.show(20, False)
    '''
    +---------+-----+------+------+----+----+
    |depName  |empNo|name  |salary|lead|lag |
    +---------+-----+------+------+----+----+
    |develop  |8    |Saga  |6000  |5200|null|
    |develop  |10   |Wilma |5200  |5200|6000|
    |develop  |11   |Maja  |5200  |4500|5200|
    |develop  |9    |Freja |4500  |4200|5200|
    |develop  |7    |Astrid|4200  |null|4500|
    |sales    |1    |Alice |5000  |4800|null|
    |sales    |3    |Ella  |4800  |4800|5000|
    |sales    |4    |Ebba  |4800  |null|4800|
    |personnel|2    |Olivia|3900  |3500|null|
    |personnel|5    |Lilly |3500  |null|3900|
    +---------+-----+------+------+----+----+
    '''

    #####################################################################
    # Running Total
    # Running Total means adding everything up until the currentRow
    #####################################################################
    overCategory = Window.partitionBy("depname").orderBy(desc("salary"))

    running_total = empsalary.withColumn(
        "rank", rank().over(overCategory)).withColumn(
        "costs", sum("salary").over(overCategory)).select(
        "depName", "empNo", "name", "salary", "rank", "costs")
    running_total.show(20, False)

    '''
    +---------+-----+------+------+----+-----+
    |depName  |empNo|name  |salary|rank|costs|
    +---------+-----+------+------+----+-----+
    |develop  |8    |Saga  |6000  |1   |6000 |
    |develop  |10   |Wilma |5200  |2   |16400|
    |develop  |11   |Maja  |5200  |2   |16400|
    |develop  |9    |Freja |4500  |4   |20900|
    |develop  |7    |Astrid|4200  |5   |25100|
    |sales    |1    |Alice |5000  |1   |5000 |
    |sales    |3    |Ella  |4800  |2   |14600|
    |sales    |4    |Ebba  |4800  |2   |14600|
    |personnel|2    |Olivia|3900  |1   |3900 |
    |personnel|5    |Lilly |3500  |2   |7400 |
    +---------+-----+------+------+----+-----+
    '''

    overCategory = Window.partitionBy("depname").orderBy(desc("salary"))
    overRowCategory = Window.partitionBy("depname").orderBy(desc("row_number"))

    running_total = empsalary.withColumn(
        "row_number", row_number().over(overCategory)).withColumn(
        "costs", sum("salary").over(overRowCategory)).select(
        "depName", "empNo", "name", "salary", "row_number", "costs")
    running_total.show(20, False)

    '''
    +---------+-----+------+------+----------+-----+
    |depName  |empNo|name  |salary|row_number|costs|
    +---------+-----+------+------+----------+-----+
    |develop  |7    |Astrid|4200  |5         |4200 |
    |develop  |9    |Freja |4500  |4         |8700 |
    |develop  |11   |Maja  |5200  |3         |13900|
    |develop  |10   |Wilma |5200  |2         |19100|
    |develop  |8    |Saga  |6000  |1         |25100|
    |sales    |4    |Ebba  |4800  |3         |4800 |
    |sales    |3    |Ella  |4800  |2         |9600 |
    |sales    |1    |Alice |5000  |1         |14600|
    |personnel|5    |Lilly |3500  |2         |3500 |
    |personnel|2    |Olivia|3900  |1         |7400 |
    +---------+-----+------+------+----------+-----+
    '''

    overCategory = Window.partitionBy("depName").rowsBetween(
        Window.currentRow, 1)
    df = empsalary.withColumn(
        "salaries", collect_list("salary").over(overCategory)).withColumn(
        "total_salary", sum("salary").over(overCategory))
    df = df.select("depName", "empNo", "name", "salary", "salaries", "total_salary")
    df.show(20, False)
    '''
    +---------+-----+------+------+------------+------------+
    |depName  |empNo|name  |salary|salaries    |total_salary|
    +---------+-----+------+------+------------+------------+
    |develop  |7    |Astrid|4200  |[4200, 6000]|10200       |
    |develop  |8    |Saga  |6000  |[6000, 4500]|10500       |
    |develop  |9    |Freja |4500  |[4500, 5200]|9700        |
    |develop  |10   |Wilma |5200  |[5200, 5200]|10400       |
    |develop  |11   |Maja  |5200  |[5200]      |5200        |
    |sales    |1    |Alice |5000  |[5000, 4800]|9800        |
    |sales    |3    |Ella  |4800  |[4800, 4800]|9600        |
    |sales    |4    |Ebba  |4800  |[4800]      |4800        |
    |personnel|2    |Olivia|3900  |[3900, 3500]|7400        |
    |personnel|5    |Lilly |3500  |[3500]      |3500        |
    +---------+-----+------+------+------------+------------+
    '''

    dfMedian = empsalary.groupBy("depName").agg(
        sort_array(collect_list("salary")).alias("salaries")).select(
        "depName", "salaries", median_udf(col("salaries")).alias("median_salary"))
    df = empsalary.join(broadcast(dfMedian), "depName").select(
        "depName", "empNo", "name", "salary", "salaries", "median_salary")
    df.show(20, False)
    '''
    +---------+-----+------+------+------------------------------+-------------+
    |depName  |empNo|name  |salary|salaries                      |median_salary|
    +---------+-----+------+------+------------------------------+-------------+
    |sales    |1    |Alice |5000  |[4800, 4800, 5000]            |4800         |
    |personnel|2    |Olivia|3900  |[3500, 3900]                  |3900         |
    |sales    |3    |Ella  |4800  |[4800, 4800, 5000]            |4800         |
    |sales    |4    |Ebba  |4800  |[4800, 4800, 5000]            |4800         |
    |personnel|5    |Lilly |3500  |[3500, 3900]                  |3900         |
    |develop  |7    |Astrid|4200  |[4200, 4500, 5200, 5200, 6000]|5200         |
    |develop  |8    |Saga  |6000  |[4200, 4500, 5200, 5200, 6000]|5200         |
    |develop  |9    |Freja |4500  |[4200, 4500, 5200, 5200, 6000]|5200         |
    |develop  |10   |Wilma |5200  |[4200, 4500, 5200, 5200, 6000]|5200         |
    |develop  |11   |Maja  |5200  |[4200, 4500, 5200, 5200, 6000]|5200         |
    +---------+-----+------+------+------------------------------+-------------+
    '''
    print("Execution Completed")