===============
Spark Streaming
===============

Overview
========

- In this tutorial, I'm showing spark streaming with 2 scenario:
  a. Reading new file from Folder
  b. Reading Tweets from Twitter web

Requirements
============

- Configure Apache Spark
- install pyspark (Python Library) using pip install pyspark
- Need to create a new app in Twitter


Execution
=============

Scenario 1: Reading Files from folder:
  a. First run fileGenerator.py, which create new files in mentioned folder.
  b. Next run the FileStreaming.py which reads the file from the location where files are getting generated.
  
Scenario 2: Reading Twitter data:
  a. First run the TweetRead.py, which extablished connection with twitter app.
  b. Next run the SparkTwitterConnect.py, which read the twitter tweets line by line which got it form above (a). Store each line in local machine
