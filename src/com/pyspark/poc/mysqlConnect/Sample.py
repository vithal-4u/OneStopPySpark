from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.sql.types import StructType, StructField, IntegerType, \
    DoubleType, StringType, FloatType

conf = BaseConfUtils()
spark = conf.createSparkSession("Testing")

def createSchema():
    eachSchema= [StructField("id", IntegerType(), True), \
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
    df_with_schema = spark.read.format("csv").option("header", "true")\
        .schema(createSchema())\
        .load("D:/Study_Document/GIT/OneStopPySpark/resources/student-dataset.csv")
    df_with_schema.printSchema()
    df_with_schema.show(20)
    df_with_schema.drop("latitude", "longitude", "language_grade").show(20)
    print("Testing")