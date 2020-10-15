'''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.

@author: Ashok Kumar
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils

conf = BaseConfUtils()
sc = conf.createSparkContext("SumOfNumbersSolution")

if __name__ == "__main__":
    primeNumbersFile = sc.textFile("D:/Study_Document/GIT/OneStopPySpark/resources/prime_nums.text")
    onlyNumber = primeNumbersFile.flatMap(lambda line: line.split("\t"))

    validNumbers = onlyNumber.filter(lambda number: number)

    intNumbers = validNumbers.map(lambda number: int(number))

    print("Sum is: {}".format(intNumbers.reduce(lambda x, y: x + y)))

    print("=================== Execution Completed ===================")