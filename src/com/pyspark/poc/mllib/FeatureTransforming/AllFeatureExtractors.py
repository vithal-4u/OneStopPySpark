'''
Created on 06-July-2020

    What is Feature Extraction?
    Feature extraction is a process of dimensionality reduction by which an initial set of raw data is
        reduced to more manageable groups for processing. A characteristic of these large data sets is
        a large number of variables that require a lot of computing resources to process.
        Feature extraction is the name for methods that select and /or combine variables into features,
        effectively reducing the amount of data that must be processed, while still accurately and
        completely describing the original data set.

    Below are feature extraction supported in PySpark:
    a. TF-IDF
    b. Word2Vec
    c. CountVectorizer
    d. FeatureHasher
@author: kasho
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.ml.feature import Word2Vec
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import FeatureHasher

conf = BaseConfUtils()
sparkSess = conf.createSparkSession("Examples of All Feature Extractions")

###############################################################################################################
# TF-IDF :
#   Term frequency-inverse document frequency (TF-IDF) is a feature vectorization method widely used in
#   text mining to reflect the importance of a term to a document in the corpus. Denote a term by t,
#   a document by d, and the corpus by D.
#
#   In MLlib, we separate TF and IDF to make them flexible:
#   a.  TF: Both HashingTF and CountVectorizer can be used to generate the term frequency vectors.
#           HashingTF is a Transformer which takes sets of terms and converts those sets into fixed-length
#           eature vectors. In text processing, a “set of terms” might be a bag of words. HashingTF utilizes
#           the hashing trick.
#
#           CountVectorizer converts text documents to vectors of term counts. Refer to CountVectorizer
#           for more details.
#   b.  IDF: IDF is an Estimator which is fit on a dataset and produces an IDFModel. The IDFModel takes
#           feature vectors (generally created from HashingTF or CountVectorizer) and scales each feature.
#           Intuitively, it down-weights features which appear frequently in a corpus.
#
###############################################################################################################
sentenceData = sparkSess.createDataFrame([
                        (0.0, "Hi I heard about Spark"),
                        (0.0, "I wish Java could use case classes"),
                        (1.0, "Logistic regression models are neat")
                    ], ["label", "sentence"])
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

rescaledData.select("label", "features").show(truncate=False)
'''
+-----+----------------------------------------------------------------------------------------------------------------------+
|label|features                                                                                                              |
+-----+----------------------------------------------------------------------------------------------------------------------+
|0.0  |(20,[0,5,9,17],[0.6931471805599453,0.6931471805599453,0.28768207245178085,1.3862943611198906])                        |
|0.0  |(20,[2,7,9,13,15],[0.6931471805599453,0.6931471805599453,0.8630462173553426,0.28768207245178085,0.28768207245178085]) |
|1.0  |(20,[4,6,13,15,18],[0.6931471805599453,0.6931471805599453,0.28768207245178085,0.28768207245178085,0.6931471805599453])|
+-----+----------------------------------------------------------------------------------------------------------------------+
'''


###############################################################################################################
# Word2Vec :
# Word2Vec is an Estimator which takes sequences of words representing documents and trains a Word2VecModel.
#   The model maps each word to a unique fixed-size vector. The Word2VecModel transforms each document into
#   a vector using the average of all words in the document; this vector can then be used as features for
#   prediction, document similarity calculations, etc. Please refer to the MLlib user guide on
#   Word2Vec for more details.
###############################################################################################################
documentDF = sparkSess.createDataFrame([\
                ("Hi I heard about Spark".split(" "), ),\
                ("Spark Mllib".split(" "), ),\
                ("I wish Java could use case classes".split(" "), ),\
                ("Logistic regression models are neat".split(" "), )\
            ], ["text"])

word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")
model = word2Vec.fit(documentDF)
result = model.transform(documentDF)
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))

'''
Text: [Hi, I, heard, about, Spark] => 
Vector: [-0.048111509531736374,-0.02916291877627373,0.06992927305400372]

Text: [Spark, Mllib] => 
Vector: [0.012607155367732048,-0.02738049626350403,0.04344792477786541]

Text: [I, wish, Java, could, use, case, classes] => 
Vector: [-0.015114391754780496,-0.03809405830023544,0.0009177655779889651]

Text: [Logistic, regression, models, are, neat] => 
Vector: [0.0031349107623100283,-0.030415994673967363,0.018867526203393936]
'''

###############################################################################################################
# CountVectorizer :
#   CountVectorizer and CountVectorizerModel aim to help convert a collection of text documents to vectors of
#   token counts. CountVectorizer can be used as an Estimator to extract the vocabulary, and generates a
#   CountVectorizerModel. The model produces sparse representations for the documents over the vocabulary,
#   which can then be passed to other algorithms like LDA. CountVectorizer will select the top vocabSize
#   words ordered by term frequency across the corpus.
###############################################################################################################

df = sparkSess.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b c a".split(" "))
], ["id", "words"])

# fit a CountVectorizerModel from the corpus.
cv = CountVectorizer(inputCol="words", outputCol="features", vocabSize=3, minDF=2.0)

model = cv.fit(df)

result = model.transform(df)
result.show(truncate=False)
'''
+---+---------------+-------------------------+
|id |words          |features                 |
+---+---------------+-------------------------+
|0  |[a, b, c]      |(3,[0,1,2],[1.0,1.0,1.0])|
|1  |[a, b, b, c, a]|(3,[0,1,2],[2.0,2.0,1.0])|
+---+---------------+-------------------------+
'''

###############################################################################################################
# FeatureHasher :
#   Feature hashing projects a set of categorical or numerical features into a feature vector of specified
#   dimension.
#   The FeatureHasher transformer operates on multiple columns. Each column may contain either numeric or
#   categorical features. Behavior and handling of column data types is as follows:
#   a. Numeric columns: For numeric features, the hash value of the column name is used to map the feature
#       value to its index in the feature vector. By default, numeric features are not treated as
#       categorical (even when they are integers). To treat them as categorical, specify the relevant
#       columns using the categoricalCols parameter.
#   b. String columns: For categorical features, the hash value of the string “column_name=value” is used to
#       map to the vector index, with an indicator value of 1.0. Thus, categorical features are “one-hot”
#       encoded (similarly to using OneHotEncoder with dropLast=false).
#   c. Boolean columns: Boolean values are treated in the same way as string columns. That is,
#       boolean features are represented as “column_name=true” or “column_name=false”,
#       with an indicator value of 1.0.
#   Null (missing) values are ignored
###############################################################################################################

dataset = sparkSess.createDataFrame([
    (2.2, True, "1", "foo"),
    (3.3, False, "2", "bar"),
    (4.4, False, "3", "baz"),
    (5.5, False, "4", "foo")
], ["real", "bool", "stringNum", "string"])

hasher = FeatureHasher(inputCols=["real", "bool", "stringNum", "string"],
                       outputCol="features")

featurized = hasher.transform(dataset)
featurized.show(truncate=False)
'''
+----+-----+---------+------+--------------------------------------------------------+
|real|bool |stringNum|string|features                                                |
+----+-----+---------+------+--------------------------------------------------------+
|2.2 |true |1        |foo   |(262144,[174475,247670,257907,262126],[2.2,1.0,1.0,1.0])|
|3.3 |false|2        |bar   |(262144,[70644,89673,173866,174475],[1.0,1.0,1.0,3.3])  |
|4.4 |false|3        |baz   |(262144,[22406,70644,174475,187923],[1.0,1.0,4.4,1.0])  |
|5.5 |false|4        |foo   |(262144,[70644,101499,174475,257907],[1.0,1.0,5.5,1.0]) |
+----+-----+---------+------+--------------------------------------------------------+
'''
print("Execution Completed")