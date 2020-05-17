'''
Created on 25-Apr-2020

Write data to MySQL DB

@author: kasho
'''
from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.sql.types import Row, StructType, StructField, IntegerType, \
    DoubleType, StringType, FloatType
from pyspark.sql.session import SparkSession

conf = BaseConfUtils()
spark = conf.createSparkSession("Write to MySQL")


def createRow(row):
    eachRow = row.split(",")
    row =Row(eachRow[0], eachRow[1], eachRow[2], \
              eachRow[3], eachRow[6], eachRow[7], \
              eachRow[8], eachRow[9], eachRow[10], \
              eachRow[12], eachRow[13], eachRow[14])

    # row = Row(id=eachRow[0], name=eachRow[1], nationality=eachRow[2], \
    #           city=eachRow[3], gender=eachRow[6], age=eachRow[7], \
    #           english_grade=eachRow[8], math_grade=eachRow[9], sciences_grade=eachRow[10], \
    #           portfolio_rating=eachRow[12], coverletter_rating=eachRow[13], refletter_rating=eachRow[14])
    print(row)
    return row


def createSchema():
    eachSchema = [StructField("id", IntegerType(), True), \
          StructField("name", StringType(), True), \
          StructField("nationality", StringType(), True), \
          StructField("city", StringType(), True), \
          StructField("latitude", FloatType(), True), \
          StructField("longitude", FloatType(), True), \
          StructField("gender", StringType(), True), \
          StructField("age", IntegerType(), True), \
          StructField("english_grade", FloatType(), True), \
          StructField("math_grade", FloatType(), True), \
          StructField("sciences_grade", FloatType(), True), \
          StructField("language_grade", IntegerType(), True), \
          StructField("portfolio_rating", IntegerType(), True), \
          StructField("coverletter_rating", IntegerType(), True), \
          StructField("refletter_rating", IntegerType(), True) \
          ]
    schema = StructType(fields=eachSchema)
    return schema


if __name__ == '__main__':
    df_with_schema = spark.read.format("csv").option("header", "true") \
        .schema(createSchema()) \
        .load("D:/Study_Document/GIT/OneStopPySpark/resources/student-dataset.csv")
    df_with_schema.printSchema()
    df_with_schema.show(20)
    df_with_schema = df_with_schema.drop("latitude", "longitude", "language_grade")
    df_with_schema.write \
        .format("jdbc").option("url", "jdbc:mysql://localhost:3306/ONESTOP_SPARK_DB") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Student_Marks") \
        .option("user", "root").option("password", "ayyappasai")\
        .mode("append")\
        .save()
    print("--- Inserted data into MySQL ---")




