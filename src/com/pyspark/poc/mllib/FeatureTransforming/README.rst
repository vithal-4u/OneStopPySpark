===============
Extracting, transforming and selecting features
===============

Overview
========

This section covers algorithms for working with features, roughly divided into these groups:

- Extraction: Extracting features from “raw” data
- Transformation: Scaling, converting, or modifying features
- Selection: Selecting a subset from a larger set of features
- Locality Sensitive Hashing (LSH): This class of algorithms combines aspects of feature transformation with other algorithms.

Feature Extractors
============
What is Feature Extraction?
Feature extraction is a process of dimensionality reduction by which an initial set of raw data is reduced to more manageable groups for processing. A characteristic of these large data sets is a large number of variables that require a lot of computing resources to process. Feature extraction is the name for methods that select and /or combine variables into features,	effectively reducing the amount of data that must be processed, while still accurately and completely describing the original data set.

    Below are feature extraction supported in PySpark:
	- TF-IDF
	- Word2Vec
	- CountVectorizer
	- FeatureHasher


TF-IDF :
=============
Term frequency-inverse document frequency (TF-IDF) is a feature vectorization method widely used in text mining to reflect the importance of a term to a document in the corpus. Denote a term by t, a document by d, and the corpus by D.
In MLlib, we separate TF and IDF to make them flexible:
	a. TF: Both HashingTF and CountVectorizer can be used to generate the term frequency vectors. HashingTF is a Transformer which takes sets of terms and converts those sets into fixed-length eature vectors. In text processing, a “set of terms” might be a bag of words. HashingTF utilizes the hashing trick. 
	
	CountVectorizer converts text documents to vectors of term counts. Refer to CountVectorizer for more details.
	b. IDF: IDF is an Estimator which is fit on a dataset and produces an IDFModel. The IDFModel takes feature vectors (generally created from HashingTF or CountVectorizer) and scales each feature. Intuitively, it down-weights features which appear frequently in a corpus.
  

Word2Vec :
=============
Word2Vec is an Estimator which takes sequences of words representing documents and trains a Word2VecModel. The model maps each word to a unique fixed-size vector. The Word2VecModel transforms each document into a vector using the average of all words in the document; this vector can then be used as features for prediction, document similarity calculations, etc. Please refer to the MLlib user guide on Word2Vec for more details.


CountVectorizer :
==================
CountVectorizer and CountVectorizerModel aim to help convert a collection of text documents to vectors of token counts. CountVectorizer can be used as an Estimator to extract the vocabulary, and generates a CountVectorizerModel. The model produces sparse representations for the documents over the vocabulary,which can then be passed to other algorithms like LDA. CountVectorizer will select the top vocabSize words ordered by term frequency across the corpus.


FeatureHasher :
==================
Feature hashing projects a set of categorical or numerical features into a feature vector of specified dimension. The FeatureHasher transformer operates on multiple columns. Each column may contain either numeric or categorical features. Behavior and handling of column data types is as follows:
  a. Numeric columns: For numeric features, the hash value of the column name is used to map the feature
      value to its index in the feature vector. By default, numeric features are not treated as
      categorical (even when they are integers). To treat them as categorical, specify the relevant
      columns using the categoricalCols parameter.
  b. String columns: For categorical features, the hash value of the string “column_name=value” is used to
      map to the vector index, with an indicator value of 1.0. Thus, categorical features are “one-hot”
      encoded (similarly to using OneHotEncoder with dropLast=false).
  c. Boolean columns: Boolean values are treated in the same way as string columns. That is,
      boolean features are represented as “column_name=true” or “column_name=false”,
      with an indicator value of 1.0.
  Null (missing) values are ignored
  
  
Feature Transformer :
========================
What is Feature Transformer?
Feature transformation is the name given to replacing our original features with functions of these features. The functions can either be of individual features, or of groups of them. The result of these functions is itself a random variable – by definition the function of any random variable is itself a random variable. Utilizing feature transformations is equivalent to changing the bases of our feature space, and it is done for the same reason that we change bases in calculus: We hope that the new bases will be easier to work with.
