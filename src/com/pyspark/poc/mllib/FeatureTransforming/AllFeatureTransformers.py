'''
Created on 10-July-2020

    What is Feature Transformer?
    Feature transformation is the name given to replacing our original features with functions of these features.
    The functions can either be of individual features, or of groups of them. The result of these functions is
    itself a random variable – by definition the function of any random variable is itself a random variable.
    Utilizing feature transformations is equivalent to changing the bases of our feature space, and it is
    done for the same reason that we change bases in calculus: We hope that the new bases will be easier to work with.

    Below are feature transformer supported in PySpark:
    a. Tokenizer
    b. StopWordsRemover
    c. n-gram
    d. Binarizer
    e. PCA
    f. PolynomialExpansion
    g. StringIndexer
@author: kasho
'''

from com.pyspark.poc.utils.BaseConfUtils import BaseConfUtils
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

conf = BaseConfUtils()
spark = conf.createSparkSession("Examples of All Feature Transformers")

###############################################################################################################
# Tokenizer :
#   Tokenization is the process of taking text (such as a sentence) and breaking it into individual terms
#   (usually words). A simple Tokenizer class provides this functionality.
#   RegexTokenizer allows more advanced tokenization based on regular expression (regex) matching. By default,
#   the parameter “pattern” (regex, default: "\\s+") is used as delimiters to split the input text.
#   Alternatively, users can set parameter “gaps” to false indicating the regex “pattern” denotes “tokens”
#   rather than splitting gaps, and find all matching occurrences as the tokenization result.
#
###############################################################################################################

sentenceDataFrame = spark.createDataFrame([\
                    (0, "Hi I heard about Spark"),\
                    (1, "I wish Java could use case classes"),\
                    (2, "Logistic,regression,models,are,neat")\
                ], ["id", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
# alternatively, pattern="\\w+", gaps(False)

countTokens = udf(lambda words: len(words), IntegerType())

tokenized = tokenizer.transform(sentenceDataFrame)
tokenized.select("sentence", "words")\
    .withColumn("tokens", countTokens(col("words"))).show(truncate=False)
'''
+-----------------------------------+------------------------------------------+------+
|sentence                           |words                                     |tokens|
+-----------------------------------+------------------------------------------+------+
|Hi I heard about Spark             |[hi, i, heard, about, spark]              |5     |
|I wish Java could use case classes |[i, wish, java, could, use, case, classes]|7     |
|Logistic,regression,models,are,neat|[logistic,regression,models,are,neat]     |1     |
+-----------------------------------+------------------------------------------+------+
'''

regexTokenized = regexTokenizer.transform(sentenceDataFrame)
regexTokenized.select("sentence", "words") \
    .withColumn("tokens", countTokens(col("words"))).show(truncate=False)
'''
+-----------------------------------+------------------------------------------+------+
|sentence                           |words                                     |tokens|
+-----------------------------------+------------------------------------------+------+
|Hi I heard about Spark             |[hi, i, heard, about, spark]              |5     |
|I wish Java could use case classes |[i, wish, java, could, use, case, classes]|7     |
|Logistic,regression,models,are,neat|[logistic, regression, models, are, neat] |5     |
+-----------------------------------+------------------------------------------+------+
'''

###############################################################################################################
# StopWordsRemover :
#   Stop words are words which should be excluded from the input, typically because the words appear frequently
#   and don’t carry as much meaning.
#
#   StopWordsRemover takes as input a sequence of strings (e.g. the output of a Tokenizer) and drops all the
#   stop words from the input sequences. The list of stopwords is specified by the stopWords parameter.
#   Default stop words for some languages are accessible by calling
#   StopWordsRemover.loadDefaultStopWords(language), for which available options are “danish”, “dutch”,
#   “english”, “finnish”, “french”, “german”, “hungarian”, “italian”, “norwegian”, “portuguese”, “russian”,
#   “spanish”, “swedish” and “turkish”. A boolean parameter caseSensitive indicates if the matches should be
#   case sensitive (false by default).
#
###############################################################################################################

from pyspark.ml.feature import StopWordsRemover

sentenceData = spark.createDataFrame([\
                (0, ["I", "saw", "the", "red", "balloon"]),\
                (1, ["Mary", "had", "a", "little", "lamb"])\
            ], ["id", "raw"])

remover = StopWordsRemover(inputCol="raw", outputCol="filtered")
remover.transform(sentenceData).show(truncate=False)
'''
+---+----------------------------+--------------------+
|id |raw                         |filtered            |
+---+----------------------------+--------------------+
|0  |[I, saw, the, red, balloon] |[saw, red, balloon] |
|1  |[Mary, had, a, little, lamb]|[Mary, little, lamb]|
+---+----------------------------+--------------------+
'''

###############################################################################################################
# n-gram :
#   An n-gram is a sequence of n tokens (typically words) for some integer n. The NGram class can be used to
#   transform input features into n-grams.
#   NGram takes as input a sequence of strings (e.g. the output of a Tokenizer). The parameter n is used to
#   determine the number of terms in each n-gram. The output will consist of a sequence of n-grams where each
#   n-gram is represented by a space-delimited string of n consecutive words. If the input sequence contains
#   fewer than n strings, no output is produced.
###############################################################################################################

from pyspark.ml.feature import NGram

wordDataFrame = spark.createDataFrame([
    (0, ["Hi", "I", "heard", "about", "Spark"]),
    (1, ["I", "wish", "Java", "could", "use", "case", "classes"]),
    (2, ["Logistic", "regression", "models", "are", "neat"])
], ["id", "words"])

ngram = NGram(n=2, inputCol="words", outputCol="ngrams")

ngramDataFrame = ngram.transform(wordDataFrame)
ngramDataFrame.select("ngrams").show(truncate=False)
'''
+------------------------------------------------------------------+
|ngrams                                                            |
+------------------------------------------------------------------+
|[Hi I, I heard, heard about, about Spark]                         |
|[I wish, wish Java, Java could, could use, use case, case classes]|
|[Logistic regression, regression models, models are, are neat]    |
+------------------------------------------------------------------+
'''

###############################################################################################################
# Binarizer :
#   Binarization is the process of thresholding numerical features to binary (0/1) features.
#   Binarizer takes the common parameters inputCol and outputCol, as well as the threshold for binarization.
#   Feature values greater than the threshold are binarized to 1.0; values equal to or less than the
#   threshold are binarized to 0.0. Both Vector and Double types are supported for inputCol.
###############################################################################################################
from pyspark.ml.feature import Binarizer

continuousDataFrame = spark.createDataFrame([\
                        (0, 0.1),\
                        (1, 0.8),\
                        (2, 0.2),\
                        (3, 0.5)\
                    ], ["id", "feature"])

binarizer = Binarizer(threshold=0.5, inputCol="feature", outputCol="binarized_feature")

binarizedDataFrame = binarizer.transform(continuousDataFrame)

print("Binarizer output with Threshold = %f" % binarizer.getThreshold())
binarizedDataFrame.show()
'''
Binarizer output with Threshold = 0.500000
+---+-------+-----------------+
| id|feature|binarized_feature|
+---+-------+-----------------+
|  0|    0.1|              0.0|
|  1|    0.8|              1.0|
|  2|    0.2|              0.0|
|  3|    0.5|              0.0|
+---+-------+-----------------+
'''
##################################################################################################################
# PCA :
#   PCA is a statistical procedure that uses an orthogonal transformation to convert a set of observations of
#   possibly correlated variables into a set of values of linearly uncorrelated variables called principal
#   components. A PCA class trains a model to project vectors to a low-dimensional space using PCA.
#   The example below shows how to project 5-dimensional feature vectors into 3-dimensional principal components.
##################################################################################################################

from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors

data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
        (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
        (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
df = spark.createDataFrame(data, ["features"])

pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(df)

result = model.transform(df).select("pcaFeatures")
result.show(truncate=False)
'''
+-----------------------------------------------------------+
|pcaFeatures                                                |
+-----------------------------------------------------------+
|[1.6485728230883807,-4.013282700516296,-5.524543751369388] |
|[-4.645104331781534,-1.1167972663619026,-5.524543751369387]|
|[-6.428880535676489,-5.337951427775355,-5.524543751369389] |
+-----------------------------------------------------------+
'''

##################################################################################################################
# PolynomialExpansion :
#   Polynomial expansion is the process of expanding your features into a polynomial space,
#   which is formulated by an n-degree combination of original dimensions. A PolynomialExpansion class provides
#   this functionality. The example below shows how to expand your features into a 3-degree polynomial space.
##################################################################################################################
from pyspark.ml.feature import PolynomialExpansion
from pyspark.ml.linalg import Vectors

df = spark.createDataFrame([
    (Vectors.dense([2.0, 1.0]),),
    (Vectors.dense([0.0, 0.0]),),
    (Vectors.dense([3.0, -1.0]),)
], ["features"])

polyExpansion = PolynomialExpansion(degree=3, inputCol="features", outputCol="polyFeatures")
polyDF = polyExpansion.transform(df)

polyDF.show(truncate=False)
'''
+----------+------------------------------------------+
|features  |polyFeatures                              |
+----------+------------------------------------------+
|[2.0,1.0] |[2.0,4.0,8.0,1.0,2.0,4.0,1.0,2.0,1.0]     |
|[0.0,0.0] |[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]     |
|[3.0,-1.0]|[3.0,9.0,27.0,-1.0,-3.0,-9.0,1.0,3.0,-1.0]|
+----------+------------------------------------------+
'''

##################################################################################################################
# StringIndexer :
#   StringIndexer encodes a string column of labels to a column of label indices.
#   StringIndexer can encode multiple columns. The indices are in [0, numLabels), and
#   four ordering options are supported: “frequencyDesc”, “frequencyAsc”, “alphabetDesc”, “alphabetAsc”.
#   Note that in case of equal frequency when under “frequencyDesc”/”frequencyAsc”, the strings are further
#   sorted by alphabet.
##################################################################################################################
from pyspark.ml.feature import StringIndexer

df = spark.createDataFrame([(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")], ["id", "category"])

indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
indexed = indexer.fit(df).transform(df)
indexed.show()
'''
+---+--------+-------------+
| id|category|categoryIndex|
+---+--------+-------------+
|  0|       a|          0.0|
|  1|       b|          2.0|
|  2|       c|          1.0|
|  3|       a|          0.0|
|  4|       a|          0.0|
|  5|       c|          1.0|
+---+--------+-------------+

“a” gets index 0 because it is the most frequent, followed by “c” with index 1 and “b” with index 2.
'''
print("Execution completed")