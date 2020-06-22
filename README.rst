===============
  PySpark
===============
Apache Spark is written in Scala programming language. PySpark has been released in order to support the collaboration of Apache Spark and Python, it actually is a Python API for Spark. In addition, PySpark, helps you interface with Resilient Distributed Datasets (RDDs) in Apache Spark and Python programming language. This has been achieved by taking advantage of the Py4j library.PySpark Logo

Py4J is a popular library which is integrated within PySpark and allows python to dynamically interface with JVM objects. PySpark features quite a few libraries for writing efficient programs. Furthermore, there are various external libraries that are also compatible. Here are some of them:

PySparkSQL
===============
A PySpark library to apply SQL-like analysis on a huge amount of structured or semi-structured data. We can also use SQL queries with PySparkSQL. It can also be connected to Apache Hive. HiveQL can be also be applied. PySparkSQL is a wrapper over the PySpark core. PySparkSQL introduced the DataFrame, a tabular representation of structured data that is similar to that of a table from a relational database management system.

MLlib
==============
MLlib is a wrapper over the PySpark and it is Spark’s machine learning (ML) library. This library uses the data parallelism technique to store and work with data. The machine-learning API provided by the MLlib library is quite easy to use. MLlib supports many machine-learning algorithms for classification, regression, clustering, collaborative filtering, dimensionality reduction, and underlying optimization primitives.

GraphFrames
===============
The GraphFrames is a purpose graph processing library that provides a set of APIs for performing graph analysis efficiently, using the PySpark core and PySparkSQL. It is optimized for fast distributed computing.
Advantages of using PySpark: • Python is very easy to learn and implement. • It provides simple and comprehensive API. • With Python, the readability of code, maintenance, and familiarity is far better. • It features various options for data visualization, which is difficult using Scala or Java.

PySpark Streaming
===============
PySpark Streaming is a scalable, fault-tolerant system that follows the RDD batch paradigm. It is basically operated in mini-batches or batch intervals which can range from 500ms to larger interval windows.

In this, Spark Streaming receives a continuous input data stream from sources like Apache Flume, Kinesis, Kafka, TCP sockets etc. These streamed data are then internally broken down into multiple smaller batches based on the batch interval and forwarded to the Spark Engine. Spark Engine processes these data batches using complex algorithms expressed with high-level functions like map, reduce, join and window. Once the processing is done, the processed batches are then pushed out to databases, filesystems, and live dashboards.

.. image:: Pyspark-streaming.png
   :width: 400px

The key abstraction for Spark Streaming is Discretized Stream (DStream). DStreams are built on RDDs facilitating the Spark developers to work within the same context of RDDs and batches to solve the streaming issues. Moreover, Spark Streaming also integrates with MLlib, SQL, DataFrames, and GraphX which widens your horizon of functionalities. Being a high-level API, Spark Streaming provides fault-tolerance “exactly-once” semantics for stateful operations. 
