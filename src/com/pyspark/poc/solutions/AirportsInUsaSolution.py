'''
Created on 02-Sept-2020
Problem Requirment:
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text. Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code, ICAO Code, Latitude,
    Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"

@author: Ashok Kumar
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from com.pyspark.poc.utils.CommanUtils import CommanUtils

conf = BaseConfUtils()
sc=conf.createSparkContext("AirportList")

def splitByComma(line: str):
    splits = CommanUtils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])

if __name__ == "__main__":
    airports = sc.textFile("D:/Study_Document/GIT/OneStopPySpark/resources/airports.txt")
    airportsInUSA = airports.filter(lambda lines: CommanUtils.COMMA_DELIMITER.split(lines)[3] == "\"United States\"")
    airportsNameAndCityNames  = airportsInUSA.map(splitByComma)
    airportsNameAndCityNames.saveAsTextFile("D:/Study_Document/GIT/OneStopPySpark/out/airports_in_usa.text")

print("Execution Completed")

