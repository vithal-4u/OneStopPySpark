'''
Created on 10-Jun-2020

Basic operation on DataFrames

@author: kasho
'''
from src.com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()
sparkSess = conf.createSparkSession("GroupByAggregateFun")

if __name__ == '__main__':
    df = sparkSess.read.csv("D:/Study_Document/GIT/OneStopPySpark/resources/sales_info.csv", inferSchema=True, header=True)
    df.printSchema()
    '''
    root
     |-- Company: string (nullable = true)
     |-- Person: string (nullable = true)
     |-- Sales: double (nullable = true)
    '''

    df.show(10)

    df.groupBy("Company").count().show()
    '''
    +-------+-----+
    |Company|count|
    +-------+-----+
    |   APPL|    4|
    |   GOOG|    3|
    |     FB|    2|
    |   MSFT|    3|
    +-------+-----+
    '''

    df.groupBy("Company").max().show()
    '''
    +-------+----------+
    |Company|max(Sales)|
    +-------+----------+
    |   APPL|     750.0|
    |   GOOG|     340.0|
    |     FB|     870.0|
    |   MSFT|     600.0|
    +-------+----------+
    
    Same way we can do below :
        1. df.groupBy("Company").min().show()
        2. df.groupBy("Company").sum().show()
    '''

    # Max sales across everything
    df.agg({'Sales': 'max'}).show()
    '''
    +----------+
    |max(Sales)|
    +----------+
    |     870.0|
    +----------+
    
    Same way we can do below:
        1. df.groupBy("Company").agg({"Sales":'max'}).show()
    '''

    ################################################################################
    # Functions:
    #   There are a variety of functions you can import from pyspark.sql.functions.
    ################################################################################
    from pyspark.sql.functions import countDistinct, avg, stddev

    df.select(countDistinct("Sales")).show()
    '''
    +---------------------+
    |count(DISTINCT Sales)|
    +---------------------+
    |                   11|
    +---------------------+
    
    Same way we can do below:
        1. df.select(countDistinct("Sales").alias("Distinct Sales")).show()
        2. df.select(avg('Sales')).show()
    '''

    df.select(stddev("Sales")).show()
    '''
    +------------------+
    |stddev_samp(Sales)|
    +------------------+
    |250.08742410799007|
    +------------------+
    '''

    from pyspark.sql.functions import format_number

    sales_std = df.select(stddev("Sales").alias('std'))
    sales_std.show()
    '''
    +------------------+
    |               std|
    +------------------+
    |250.08742410799007|
    +------------------+
    '''

    # format_number("col_name",decimal places)
    sales_std.select(format_number('std', 2)).show()
    '''
    +---------------------+
    |format_number(std, 2)|
    +---------------------+
    |               250.09|
    +---------------------+
    '''

    ################################################################
    # Order By:
    #   You can easily sort with the orderBy method:
    ################################################################

    # OrderBy Ascending
    df.orderBy("Sales").show()
    '''
    +-------+-------+-----+
    |Company| Person|Sales|
    +-------+-------+-----+
    |   GOOG|Charlie|120.0|
    |   MSFT|    Amy|124.0|
    |   APPL|  Linda|130.0|
    |   GOOG|    Sam|200.0|
    |   MSFT|Vanessa|243.0|
    |   APPL|   John|250.0|
    |   GOOG|  Frank|340.0|
    |     FB|  Sarah|350.0|
    |   APPL|  Chris|350.0|
    |   MSFT|   Tina|600.0|
    |   APPL|   Mike|750.0|
    |     FB|   Carl|870.0|
    +-------+-------+-----+
    '''

    # Descending call off the column itself.
    df.orderBy(df["Sales"].desc()).show()
    '''
    +-------+-------+-----+
    |Company| Person|Sales|
    +-------+-------+-----+
    |     FB|   Carl|870.0|
    |   APPL|   Mike|750.0|
    |   MSFT|   Tina|600.0|
    |     FB|  Sarah|350.0|
    |   APPL|  Chris|350.0|
    |   GOOG|  Frank|340.0|
    |   APPL|   John|250.0|
    |   MSFT|Vanessa|243.0|
    |   GOOG|    Sam|200.0|
    |   APPL|  Linda|130.0|
    |   MSFT|    Amy|124.0|
    |   GOOG|Charlie|120.0|
    +-------+-------+-----+
    '''
    print("Execution completed")