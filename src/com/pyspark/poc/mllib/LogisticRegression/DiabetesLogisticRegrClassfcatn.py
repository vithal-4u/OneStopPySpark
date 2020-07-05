'''
Created on 27-Jun-2020

Below Problem statement:
    To buid a machine learning model to accurately predict whether
        or not the patients in the dataset have diabetes?

    Data Details:
        1) Pregnancies: Number of times pregnant (numeric)
        2) Glucose: Plasma glucose concentration a 2 hours in an oral glucose tolerance test (numeric)
        3) Blood Pressure: Diastolic blood pressure (mm Hg) (numeric)
        4) Skin Thickness: Triceps skin fold thickness (mm) (numeric)
        5) Insulin: 2-Hour serum insulin (mu U/ml) (numeric)
        6) BMI: Body mass index (weight in kg/(height in m)^2) (numeric)
        7) Diabetes Pedigree Function: Diabetes pedigree function (numeric)
        8) Age: Age of the person (years)
        9) Outcome: Class variable (0 or 1)

@author: kasho
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
import numpy as np
from pyspark.sql.functions import when
from pyspark.ml.feature import Imputer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

conf = BaseConfUtils()
sparkSess = conf.createSparkSession("Diabates data Classification")

raw_data = sparkSess.read.csv('D:/Study_Document/GIT/OneStopPySpark/resources/diabetes.csv', inferSchema=True,
                     header=True)

print(raw_data.columns)

raw_data.show(5,truncate=False)
'''
+-----------+-------+-------------+-------------+-------+----+------------------------+---+-------+
|Pregnancies|Glucose|BloodPressure|SkinThickness|Insulin|BMI |DiabetesPedigreeFunction|Age|Outcome|
+-----------+-------+-------------+-------------+-------+----+------------------------+---+-------+
|6          |148    |72           |35           |0      |33.6|0.627                   |50 |1      |
|1          |85     |66           |29           |0      |26.6|0.351                   |31 |0      |
|8          |183    |64           |0            |0      |23.3|0.672                   |32 |1      |
|1          |89     |66           |23           |94     |28.1|0.167                   |21 |0      |
|0          |137    |40           |35           |168    |43.1|2.288                   |33 |1      |
+-----------+-------+-------------+-------------+-------+----+------------------------+---+-------+
'''

raw_data.describe().select("Summary","Pregnancies","Glucose","BloodPressure").show()
'''
+-------+------------------+-----------------+------------------+
|Summary|       Pregnancies|          Glucose|     BloodPressure|
+-------+------------------+-----------------+------------------+
|  count|               768|              768|               768|
|   mean|3.8450520833333335|     120.89453125|       69.10546875|
| stddev|  3.36957806269887|31.97261819513622|19.355807170644777|
|    min|                 0|                0|                 0|
|    max|                17|              199|               122|
+-------+------------------+-----------------+------------------+
'''

raw_data.describe().select("Summary","SkinThickness","Insulin").show()
'''
+-------+------------------+------------------+
|Summary|     SkinThickness|           Insulin|
+-------+------------------+------------------+
|  count|               768|               768|
|   mean|20.536458333333332| 79.79947916666667|
| stddev|15.952217567727642|115.24400235133803|
|    min|                 0|                 0|
|    max|                99|               846|
+-------+------------------+------------------+
'''

raw_data.describe().select("Summary","BMI","DiabetesPedigreeFunction","Age").show()

'''
+-------+------------------+------------------------+------------------+
|Summary|               BMI|DiabetesPedigreeFunction|               Age|
+-------+------------------+------------------------+------------------+
|  count|               768|                     768|               768|
|   mean|31.992578124999977|      0.4718763020833327|33.240885416666664|
| stddev| 7.884160320375441|       0.331328595012775|11.760231540678689|
|    min|               0.0|                   0.078|                21|
|    max|              67.1|                    2.42|                81|
+-------+------------------+------------------------+------------------+
'''

########################################################################################################
#Looking at the above tables, it is observed that the minimum value for
#   the fields such as "Pregnancies", "glucose", "blood pressure",
#   "skin thickness","insulin" and "BMI" are zero (0) which seems impractical
#   to me (except "Pregnancies").
#
#   Therefore, let us replace all the zeros in the abaove mentioned fields (except "Pregnancies") with NaN.
########################################################################################################

raw_data=raw_data.withColumn("Glucose",when(raw_data.Glucose==0,np.nan).otherwise(raw_data.Glucose))
raw_data=raw_data.withColumn("BloodPressure",when(raw_data.BloodPressure==0,np.nan).otherwise(raw_data.BloodPressure))
raw_data=raw_data.withColumn("SkinThickness",when(raw_data.SkinThickness==0,np.nan).otherwise(raw_data.SkinThickness))
raw_data=raw_data.withColumn("BMI",when(raw_data.BMI==0,np.nan).otherwise(raw_data.BMI))
raw_data=raw_data.withColumn("Insulin",when(raw_data.Insulin==0,np.nan).otherwise(raw_data.Insulin))

raw_data.select("Insulin","Glucose","BloodPressure","SkinThickness","BMI").show(5)
'''
+-------+-------+-------------+-------------+----+
|Insulin|Glucose|BloodPressure|SkinThickness| BMI|
+-------+-------+-------------+-------------+----+
|    NaN|  148.0|         72.0|         35.0|33.6|
|    NaN|   85.0|         66.0|         29.0|26.6|
|    NaN|  183.0|         64.0|          NaN|23.3|
|   94.0|   89.0|         66.0|         23.0|28.1|
|  168.0|  137.0|         40.0|         35.0|43.1|
+-------+-------+-------------+-------------+----+
'''
########################################################################################################
# So we have replaced all "0" with NaN. Now, we can simply impute the NaN by calling an imputer.
# Note: imputer means In statistics, imputation is the process of replacing missing data
#           with substituted values..
########################################################################################################
imputer = Imputer(inputCols=["Glucose","BloodPressure","SkinThickness","BMI","Insulin"],\
                outputCols=["Glucose","BloodPressure","SkinThickness","BMI","Insulin"])
model=imputer.fit(raw_data)
raw_data=model.transform(raw_data)
raw_data.show(5)

'''
+-----------+-------+-------------+------------------+-----------------+----+------------------------+---+-------+
|Pregnancies|Glucose|BloodPressure|     SkinThickness|          Insulin| BMI|DiabetesPedigreeFunction|Age|Outcome|
+-----------+-------+-------------+------------------+-----------------+----+------------------------+---+-------+
|          6|  148.0|         72.0|              35.0|155.5482233502538|33.6|                   0.627| 50|      1|
|          1|   85.0|         66.0|              29.0|155.5482233502538|26.6|                   0.351| 31|      0|
|          8|  183.0|         64.0|29.153419593345657|155.5482233502538|23.3|                   0.672| 32|      1|
|          1|   89.0|         66.0|              23.0|             94.0|28.1|                   0.167| 21|      0|
|          0|  137.0|         40.0|              35.0|            168.0|43.1|                   2.288| 33|      1|
+-----------+-------+-------------+------------------+-----------------+----+------------------------+---+-------+
'''

########################################################################################################
# In addition, If we see the "Pregnancies" column in raw_data it can be seen that the maximum count
#   goes upto 17, which is quit unbelievable. This may be an outlier, but we will discuss on outlier
#   detection and removal in some other time.
#
#   Let us combine all the features in one single feature vector.
########################################################################################################

cols=raw_data.columns
cols.remove("Outcome")

# Let us import the vector assembler
assembler = VectorAssembler(inputCols=cols,outputCol="features")

# Now let us use the transform method to transform our dataset
raw_data=assembler.transform(raw_data)
raw_data.select("features").show(truncate=False)
'''
+-----------------------------------------------------------------------------------+
|features                                                                           |
+-----------------------------------------------------------------------------------+
|[6.0,148.0,72.0,35.0,155.5482233502538,33.6,0.627,50.0]                            |
|[1.0,85.0,66.0,29.0,155.5482233502538,26.6,0.351,31.0]                             |
|[8.0,183.0,64.0,29.153419593345657,155.5482233502538,23.3,0.672,32.0]              |
|[1.0,89.0,66.0,23.0,94.0,28.1,0.167,21.0]                                          |
|[0.0,137.0,40.0,35.0,168.0,43.1,2.288,33.0]                                        |
|[5.0,116.0,74.0,29.153419593345657,155.5482233502538,25.6,0.201,30.0]              |
|[3.0,78.0,50.0,32.0,88.0,31.0,0.248,26.0]                                          |
|[10.0,115.0,72.40518417462484,29.153419593345657,155.5482233502538,35.3,0.134,29.0]|
|[2.0,197.0,70.0,45.0,543.0,30.5,0.158,53.0]                                        |
|[8.0,125.0,96.0,29.153419593345657,155.5482233502538,32.45746367239099,0.232,54.0] |
|[4.0,110.0,92.0,29.153419593345657,155.5482233502538,37.6,0.191,30.0]              |
|[10.0,168.0,74.0,29.153419593345657,155.5482233502538,38.0,0.537,34.0]             |
|[10.0,139.0,80.0,29.153419593345657,155.5482233502538,27.1,1.441,57.0]             |
|[1.0,189.0,60.0,23.0,846.0,30.1,0.398,59.0]                                        |
|[5.0,166.0,72.0,19.0,175.0,25.8,0.587,51.0]                                        |
|[7.0,100.0,72.40518417462484,29.153419593345657,155.5482233502538,30.0,0.484,32.0] |
|[0.0,118.0,84.0,47.0,230.0,45.8,0.551,31.0]                                        |
|[7.0,107.0,74.0,29.153419593345657,155.5482233502538,29.6,0.254,31.0]              |
|[1.0,103.0,30.0,38.0,83.0,43.3,0.183,33.0]                                         |
|[1.0,115.0,70.0,30.0,96.0,34.6,0.529,32.0]                                         |
+-----------------------------------------------------------------------------------+
'''
##################################################################################
# StandardScaler to scalerize the newly created "feature" column
##################################################################################
standardScalar = StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
raw_data = standardScalar.fit(raw_data).transform(raw_data)
raw_data.select("features","Scaled_features").show(5,truncate=False)
'''
+---------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
|[6.0,148.0,72.0,35.0,155.5482233502538,33.6,0.627,50.0]              |[1.7806383732194306,4.862670805688543,5.952210601826984,3.9813708583353558,1.8295247783934943,4.887165154544966,1.8923811872495484,4.251616970894646] |
|[1.0,85.0,66.0,29.0,155.5482233502538,26.6,0.351,31.0]               |[0.29677306220323846,2.7927501248886903,5.456193051674735,3.29885013976358,1.8295247783934943,3.869005747348098,1.0593712866420917,2.6360025219546803]|
|[8.0,183.0,64.0,29.153419593345657,155.5482233502538,23.3,0.672,32.0]|[2.3741844976259077,6.0126267394662385,5.290853868290652,3.316302148279125,1.8295247783934943,3.3890163125267176,2.0281980188703295,2.721034861372573]|
|[1.0,89.0,66.0,23.0,94.0,28.1,0.167,21.0]                            |[0.29677306220323846,2.9241736601775696,5.456193051674735,2.616329421191805,1.1056078010080843,4.087182763175998,0.5040313529037872,1.785679127775751]|
|[0.0,137.0,40.0,35.0,168.0,43.1,2.288,33.0]                          |[0.0,4.501256083644124,3.3067836676816578,3.9813708583353558,1.975979899674023,6.268952921455001,6.905531349963264,2.806067200790466]                 |
+---------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
'''
#Split data into Training and Test
train, test = raw_data.randomSplit([0.8, 0.2], seed=12345)

#Let us check whether their is imbalance in the dataset
dataset_size = float(train.select("Outcome").count())
numPositives = train.select("Outcome").where('Outcome == 1').count()
per_ones = (float(numPositives)/float(dataset_size))*100
numNegatives = float(dataset_size-numPositives)
print('The number of ones are {}'.format(numPositives))
print('The number of zeros are {}'.format(numNegatives))
print('Percentage of ones are {}'.format(per_ones))
print('The number of dataset_size are {}'.format(dataset_size))
'''
The number of ones are 206
The number of zeros are 395.0
Percentage of ones are 34.27620632279534
The number of dataset_size are 601.0
'''

BalancingRatio= numNegatives/dataset_size
print('BalancingRatio = {}'.format(BalancingRatio))
'''
BalancingRatio = 0.6572379367720466
'''

train=train.withColumn("classWeights", when(train.Outcome == 1,BalancingRatio).otherwise(1-BalancingRatio))
train.select("classWeights").show(5)
'''
+-------------------+
|       classWeights|
+-------------------+
|0.34276206322795344|
|0.34276206322795344|
|0.34276206322795344|
|0.34276206322795344|
|0.34276206322795344|
+-------------------+
'''

css = ChiSqSelector(featuresCol='Scaled_features', outputCol='Aspect', labelCol='Outcome',fpr=0.05)
train = css.fit(train).transform(train)
test = css.fit(test).transform(test)
test.select("Aspect").show(5,truncate=False)
'''
+---------------------------------------------------------------------------------------------------------------------------------------+
|Aspect                                                                                                                                 |
+---------------------------------------------------------------------------------------------------------------------------------------+
|[0.0,2.562758938133151,7.274924068899646,3.29885013976358,0.47047140468429116,5.367154589366346,1.3098778871870878,1.785679127775751]  |
|[0.0,2.8256060087109103,5.621532235058818,3.640110499049468,1.8295247783934943,5.207158111092553,0.7183201316832416,2.125808485447323] |
|[0.0,2.9898854278220095,5.621532235058818,3.640110499049468,2.4699748745925287,5.8035086210221465,1.1499158410559458,2.125808485447323]|
|[0.0,3.1213089631108892,5.290853868290652,4.436384670716539,1.2349874372962644,6.487129937282901,1.1046435638490186,1.870711467193644] |
|[0.0,3.1213089631108892,6.6135673353633155,5.118905389288314,1.0820842307738696,5.30897405181224,0.995990098552394,2.210840824865216]  |
+---------------------------------------------------------------------------------------------------------------------------------------+
'''

##############################################################################################
#
# Building a classification model using Logistic Regression
#
##############################################################################################
lr = LogisticRegression(labelCol="Outcome", featuresCol="Aspect",weightCol="classWeights",maxIter=10)
model = lr.fit(train)
predict_train=model.transform(train)
predict_test=model.transform(test)
predict_test.select("Outcome","prediction").show(10,truncate=False)
'''
+-------+----------+
|Outcome|prediction|
+-------+----------+
|0      |0.0       |
|0      |0.0       |
|0      |0.0       |
|0      |0.0       |
|0      |0.0       |
|1      |0.0       |
|0      |0.0       |
|0      |0.0       |
|0      |0.0       |
|0      |1.0       |
+-------+----------+
'''
## Predication can be done as below also..
op = model.evaluate(test)
op.predictions.show(15)
'''

+-----------+-------+-----------------+------------------+-----------------+----+------------------------+---+-------+--------------------+--------------------+--------------------+--------------------+--------------------+----------+
|Pregnancies|Glucose|    BloodPressure|     SkinThickness|          Insulin| BMI|DiabetesPedigreeFunction|Age|Outcome|            features|     Scaled_features|              Aspect|       rawPrediction|         probability|prediction|
+-----------+-------+-----------------+------------------+-----------------+----+------------------------+---+-------+--------------------+--------------------+--------------------+--------------------+--------------------+----------+
|          0|   78.0|             88.0|              29.0|             40.0|36.9|                   0.434| 21|      0|[0.0,78.0,88.0,29...|[0.0,2.5627589381...|[0.0,2.5627589381...|[2.58888023648551...|[0.93014249279728...|       0.0|
|          0|   86.0|             68.0|              32.0|155.5482233502538|35.8|                   0.238| 25|      0|[0.0,86.0,68.0,32...|[0.0,2.8256060087...|[0.0,2.8256060087...|[2.32098145085482...|[0.91059987057264...|       0.0|
|          0|   91.0|             68.0|              32.0|            210.0|39.9|                   0.381| 25|      0|[0.0,91.0,68.0,32...|[0.0,2.9898854278...|[0.0,2.9898854278...|[1.68081620680194...|[0.84301258010142...|       0.0|
|          0|   95.0|             64.0|              39.0|            105.0|44.6|                   0.366| 22|      0|[0.0,95.0,64.0,39...|[0.0,3.1213089631...|[0.0,3.1213089631...|[0.64946166218389...|[0.65688913922505...|       0.0|
|          0|   95.0|             80.0|              45.0|             92.0|36.5|                    0.33| 26|      0|[0.0,95.0,80.0,45...|[0.0,3.1213089631...|[0.0,3.1213089631...|[1.78997774283908...|[0.85692454770533...|       0.0|
|          0|   95.0|             85.0|              25.0|             36.0|37.4|                   0.247| 24|      1|[0.0,95.0,85.0,25...|[0.0,3.1213089631...|[0.0,3.1213089631...|[1.78751030328341...|[0.85662176109874...|       0.0|
|          0|  105.0|             64.0|              41.0|            142.0|41.5|                   0.173| 22|      0|[0.0,105.0,64.0,4...|[0.0,3.4498678013...|[0.0,3.4498678013...|[0.76152039796475...|[0.68168373758734...|       0.0|
|          0|  107.0|             60.0|              25.0|155.5482233502538|26.4|                   0.133| 23|      0|[0.0,107.0,60.0,2...|[0.0,3.5155795689...|[0.0,3.5155795689...|[2.20547046216823...|[0.90073968569843...|       0.0|
|          0|  107.0|             76.0|29.153419593345657|155.5482233502538|45.3|                   0.686| 24|      0|[0.0,107.0,76.0,2...|[0.0,3.5155795689...|[0.0,3.5155795689...|[0.11354963419352...|[0.52835694669020...|       0.0|
|          0|  117.0|             80.0|              31.0|             53.0|45.2|                   0.089| 24|      0|[0.0,117.0,80.0,3...|[0.0,3.8441384071...|[0.0,3.8441384071...|[-0.0034952562559...|[0.49912618682562...|       1.0|
|          0|  119.0|72.40518417462484|29.153419593345657|155.5482233502538|32.4|                   0.141| 24|      1|[0.0,119.0,72.405...|[0.0,3.9098501748...|[0.0,3.9098501748...|[1.27265972668724...|[0.78119770927335...|       0.0|
|          0|  126.0|             86.0|              27.0|            120.0|27.4|                   0.515| 21|      0|[0.0,126.0,86.0,2...|[0.0,4.1398413615...|[0.0,4.1398413615...|[1.33537162886170...|[0.79172777707898...|       0.0|
|          0|  131.0|72.40518417462484|29.153419593345657|155.5482233502538|43.2|                    0.27| 26|      1|[0.0,131.0,72.405...|[0.0,4.3041207807...|[0.0,4.3041207807...|[-0.5485183479861...|[0.36620823185025...|       1.0|
|          0|  132.0|             78.0|29.153419593345657|155.5482233502538|32.4|                   0.393| 21|      0|[0.0,132.0,78.0,2...|[0.0,4.3369766645...|[0.0,4.3369766645...|[0.56241664579532...|[0.63701152088754...|       0.0|
|          0|  135.0|             68.0|              42.0|            250.0|42.3|                   0.365| 24|      1|[0.0,135.0,68.0,4...|[0.0,4.4355443159...|[0.0,4.4355443159...|[-0.5788535559218...|[0.35919643307327...|       1.0|
+-----------+-------+-----------------+------------------+-----------------+----+------------------------+---+-------+--------------------+--------------------+--------------------+--------------------+--------------------+----------+
'''

##############################################################################################
# Evaluating the model
# Now let us evaluate the model using BinaryClassificationEvaluator class in Spark ML.
#  BinaryClassificationEvaluator by default uses areaUnderROC as the performance metric
#
##############################################################################################

# The BinaryClassificationEvaluator uses areaUnderROC as the default metric. As o fnow we will continue with the same
evaluator=BinaryClassificationEvaluator(rawPredictionCol="rawPrediction",labelCol="Outcome")
predict_test.select("Outcome","rawPrediction","prediction","probability").show(5,truncate=False)
'''
+-------+----------------------------------------+----------+----------------------------------------+
|Outcome|rawPrediction                           |prediction|probability                             |
+-------+----------------------------------------+----------+----------------------------------------+
|0      |[2.5888802364855197,-2.5888802364855197]|0.0       |[0.9301424927972886,0.06985750720271146]|
|0      |[2.3209814508548243,-2.3209814508548243]|0.0       |[0.9105998705726448,0.08940012942735513]|
|0      |[1.6808162068019445,-1.6808162068019445]|0.0       |[0.8430125801014288,0.15698741989857123]|
|0      |[0.649461662183894,-0.649461662183894]  |0.0       |[0.6568891392250562,0.34311086077494374]|
|0      |[1.789977742839083,-1.789977742839083]  |0.0       |[0.8569245477053379,0.14307545229466212]|
+-------+----------------------------------------+----------+----------------------------------------+
'''

print("The area under ROC for train set is {}".format(evaluator.evaluate(predict_train)))
print("The area under ROC for test set is {}".format(evaluator.evaluate(predict_test)))
'''
The area under ROC for train set is 0.838687476957108
The area under ROC for test set is 0.8447004608294926
'''
print("Execution completed")