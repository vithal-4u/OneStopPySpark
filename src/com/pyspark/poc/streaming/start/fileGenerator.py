'''
Created on 16-Jun-2020

This is use for create 30 file one by one in each 5 seconds interval.
These files will store content dynamically from 'lorem.txt' using below code

@author: kasho
'''

from random import randint
import time

def main():
    a = 1
    with open("D:/Study_Document/GIT/OneStopPySpark/resources/wordCount.txt", "r") as file:  # reading content from 'lorem.txt' file
        lines = file.readlines()
        while a <= 30:
            totalline = len(lines)
            linenumber = randint(0, totalline - 10)
            with open('D:/Study_Document/GIT/OneStopPySpark/temp/log{}.txt'.format(a), 'w') as writefile:
                writefile.write(' '.join(line for line in lines[linenumber:totalline]))
            print('creating file log{}.txt'.format(a))
            a += 1
            time.sleep(5)


if __name__ == "__main__":
    main()