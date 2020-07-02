'''
Created on 27-Jun-2020

Below Problem statement:
    A marketing agency has many customers that use their service to produce ads
    for the client/customer websites. They've noticed that they have quite a bit of churn in clients.
    They basically randomly assign account managers right now, but want you to
    create a machine learning model that will help predict which customers will
    churn (stop buying their service) so that they can correctly assign the customers most
    at risk to churn an account manager. Luckily they have some historical data, can you help them out?
    Create a classification algorithm that will help classify whether or not a customer churned.
    Then the company can test this against incoming data for future customers to predict
    which customers will churn and assign them an account manager.

    The data is saved as customer_churn.csv. Here are the fields and their definitions:
        Name : Name of the latest contact at Company
        Age: Customer Age
        Total_Purchase: Total Ads Purchased
        Account_Manager: Binary 0=No manager, 1= Account manager assigned
        Years: Totaly Years as a customer
        Num_sites: Number of websites that use the service.
        Onboard_date: Date that the name of the latest contact was onboarded
        Location: Client HQ Address
        Company: Name of Client Company

@author: kasho
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

conf = BaseConfUtils()
sparkSess = conf.createSparkSession("customer churn Model")

if __name__ == "__main__":
    data = sparkSess.read.csv('D:/Study_Document/GIT/OneStopPySpark/resources/customer_churn.csv', inferSchema=True,
                     header=True)
    data.printSchema()
    data.show(5)
    '''
    +----------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
    |           Names| Age|Total_Purchase|Account_Manager|Years|Num_Sites|       Onboard_date|            Location|             Company|Churn|
    +----------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
    |Cameron Williams|42.0|       11066.8|              0| 7.22|      8.0|2013-08-30 07:00:40|10265 Elizabeth M...|          Harvey LLC|    1|
    |   Kevin Mueller|41.0|      11916.22|              0|  6.5|     11.0|2013-08-13 00:38:46|6157 Frank Garden...|          Wilson PLC|    1|
    |     Eric Lozano|38.0|      12884.75|              0| 6.67|     12.0|2016-06-29 06:20:07|1331 Keith Court ...|Miller, Johnson a...|    1|
    |   Phillip White|42.0|       8010.76|              0| 6.71|     10.0|2014-04-22 12:43:12|13120 Daniel Moun...|           Smith Inc|    1|
    |  Cynthia Norton|37.0|       9191.58|              0| 5.56|      9.0|2016-01-19 15:31:15|765 Tricia Row Ka...|          Love-Jones|    1|
    +----------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
    '''
    print(data.columns)

    assembler = VectorAssembler(inputCols=['Age','Total_Purchase','Account_Manager','Years','Num_Sites'],outputCol='features')
    output = assembler.transform(data)
    output.show(5, truncate=False)

    final_data = output.select('features', 'churn')
    final_data.show(5, truncate=False)
    '''
    +-----------------------------+-----+
    |features                     |churn|
    +-----------------------------+-----+
    |[42.0,11066.8,0.0,7.22,8.0]  |1    |
    |[41.0,11916.22,0.0,6.5,11.0] |1    |
    |[38.0,12884.75,0.0,6.67,12.0]|1    |
    |[42.0,8010.76,0.0,6.71,10.0] |1    |
    |[37.0,9191.58,0.0,5.56,9.0]  |1    |
    +-----------------------------+-----+
    '''
    print(final_data)

    train_churn, test_churn = final_data.randomSplit([0.7, 0.3])

    lr_churn = LogisticRegression(labelCol='churn')
    fitted_churn_model = lr_churn.fit(train_churn)
    training_sum = fitted_churn_model.summary
    training_sum.predictions.describe().show()
    '''
    +-------+-------------------+-------------------+
    |summary|              churn|         prediction|
    +-------+-------------------+-------------------+
    |  count|                614|                614|
    |   mean|0.16938110749185667|0.13680781758957655|
    | stddev| 0.3753940068686483|0.34392453201749257|
    |    min|                0.0|                0.0|
    |    max|                1.0|                1.0|
    +-------+-------------------+-------------------+
    '''

    pred_and_labels = fitted_churn_model.evaluate(test_churn)
    pred_and_labels.predictions.show(5)
    '''
    +--------------------+-----+--------------------+--------------------+----------+
    |            features|churn|       rawPrediction|         probability|prediction|
    +--------------------+-----+--------------------+--------------------+----------+
    |[26.0,8787.39,1.0...|    1|[0.70949033009277...|[0.67028853161135...|       0.0|
    |[28.0,9090.43,1.0...|    0|[1.60680005230690...|[0.83296664165673...|       0.0|
    |[29.0,8688.17,1.0...|    1|[2.80779063825278...|[0.94309536649581...|       0.0|
    |[29.0,11274.46,1....|    0|[4.84880648963315...|[0.99222322585103...|       0.0|
    |[29.0,12711.15,0....|    0|[5.55804421393591...|[0.99615850286160...|       0.0|
    +--------------------+-----+--------------------+--------------------+----------+
    '''
    churn_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='churn')
    accurasy = churn_eval.evaluate(pred_and_labels.predictions)
    print(accurasy)
    ################################################################################################
    # Accuracy for above Model - 0.7350649350649351 i.e. 74%
    ################################################################################################

    ##########################################
    # Predict on brand new unlabeled data
    ##########################################

    final_lr_model = lr_churn.fit(final_data)
    new_customers = sparkSess.read.csv('D:/Study_Document/GIT/OneStopPySpark/resources/new_customers.csv', inferSchema=True,
                              header=True)
    test_new_customers = assembler.transform(new_customers)
    final_results = final_lr_model.transform(test_new_customers)
    final_results.select('Company', 'prediction').show(5)
    '''
    Final prediction from new dataset from prepared model.
    +----------------+----------+
    |         Company|prediction|
    +----------------+----------+
    |        King Ltd|       0.0|
    |   Cannon-Benson|       1.0|
    |Barron-Robertson|       1.0|
    |   Sexton-Golden|       1.0|
    |        Wood LLC|       0.0|
    +----------------+----------+
    '''
    print("Execution completed")