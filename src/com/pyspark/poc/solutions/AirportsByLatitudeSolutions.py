'''
Created on 03-Sept-2020
    Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
    Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

    Each row of the input file contains the following columns:
        Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code, ICAO Code,
        Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "St Anthony", 51.391944
    "Tofino", 49.082222

'''
from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from com.pyspark.poc.utils.CommanUtils import CommanUtils

conf = BaseConfUtils()
sc = conf.createSparkContext("AirportsLatitude")

def splitByComma(line: str):
    splits = CommanUtils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    airportsLatitude = sc.textFile("D:/Study_Document/GIT/OneStopPySpark/resources/airports.txt")
    filteredAirports = airportsLatitude.filter(lambda lines: float(CommanUtils.COMMA_DELIMITER.split(lines)[6]) > 40)
    airpotsNameAndLatitude = filteredAirports.map(splitByComma)
    airpotsNameAndLatitude.saveAsTextFile("D:/Study_Document/GIT/OneStopPySpark/out/airportsLatitude_in_usa.text")

print("Execution Completed")