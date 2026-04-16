import pyspark
from pyspark.sql.functions import col,isnan, when, count
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import rand
from pyspark.sql.types import DoubleType
from delta import *
from delta.tables import DeltaTable
from pyspark.ml.feature import Word2Vec
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import udf
from pyspark.ml.feature import StopWordsRemover
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('snowball_data')
from nltk.stem.snowball import SnowballStemmer
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import col
import sparknlp
from sparknlp.annotator import Stemmer
from pyspark.ml.linalg import DenseVector
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.types import StringType
from pyspark.sql.functions import isnan, sum
from pyspark.ml.linalg import VectorUDT
import numpy as np
from pyspark.sql.functions import length
from pyspark.ml.evaluation import ClusteringEvaluator
import functools

def createSparkSession(master_url): #Create the dataframe
    builder = pyspark.sql.SparkSession.builder.master(master_url).appName('Sentiment_analysis').config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

def createDf(spark):
    schema = pyspark.sql.types.StructType().add('created_at', 'string').add('text', 'string').add('favourites_count', 'string').add('quote_count', 'string').add('reply_count', 'string').add('retweet_count', 'string')
    schema2 = pyspark.sql.types.StructType().add('created_at', 'string').add('text', 'string').add('favourites_count', 'string').add('retweet_count', 'string')

    tweetData = spark.read.option("mode", "DROPMALFORMED").csv('hdfs://192.168.3.107:9000/datasets/csvOutput/part*', schema=schema)
    tweetData2 = spark.read.option("mode", "DROPMALFORMED").csv('hdfs://192.168.3.107:9000/datasets/merged_csv.csv', schema=schema2)

    tweetData = tweetData.drop('quote_count', 'reply_count')

    tweetData = tweetData.withColumn('favourites_count', tweetData.favourites_count.cast('int'))
    tweetData = tweetData.withColumn('retweet_count', tweetData.retweet_count.cast('int'))

    finalTweetData = unionAll([tweetData, tweetData2])

    return finalTweetData


def preProccesing(df): #Delete rows where null values > 4 and split the dataset.

    df = df.dropna(how='any', thresh=4)
    df_train, df_test = df.randomSplit([0.7, 0.3], seed=100)

    return df_train, df_test

master_url = "spark://192.168.3.107:7077"
spark = createSparkSession(master_url)
tweetData = createDf(spark)
train_tweet, test_tweet = preProccesing(tweetData)

#-------------------------------------Words to vectors model

tokenizer = Tokenizer(inputCol="text", outputCol="words")
stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
preprocessed_data_train = train_tweet.select("text")
preprocessed_data_train = stop_words_remover.transform(tokenizer.transform(preprocessed_data_train))
preprocessed_data_train = preprocessed_data_train.select("filtered_words") #only 1 column now 
#preprocessed_data_train.show()

word2vec = Word2Vec(vectorSize=50, minCount=15, numPartitions=500, maxIter=1, inputCol="filtered_words", outputCol="word_vectors") #Try minPartition = 500 first run.
word2vec_model = word2vec.fit(preprocessed_data_train) #train on the given dataset

vectors = word2vec_model.getVectors()
#print(vectors.printSchema())
#print(vectors.show())

#Transform preprocessed_data_train using the word2vec_model
preprocessed_data_train = word2vec_model.transform(preprocessed_data_train)
#preprocessed_data_train.show()


#--------------------------------------------------- MODEL



# Create KMeans instance
kmeans = KMeans(k=3, seed=1, featuresCol='word_vectors')

# Fit the KMeans model on the word vectors
model = kmeans.fit(preprocessed_data_train)

# Get the cluster centers
centers = model.clusterCenters()

# Get the cluster assignments for each tweet
predictions = model.transform(preprocessed_data_train)

predictions = predictions.dropDuplicates(['filtered_words'])

# Show the predictions
predictions.show()


#--------------------------------------------------- EVALUATION OF TRAIN


# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator(featuresCol='word_vectors')

# Evaluate the model on training data
silhouette_score = evaluator.evaluate(predictions)
print("Silhouette score:", silhouette_score)

# Get the within-cluster sum of squared distances (WSSSE) of the fitted model on the training data
wssse = model.summary.trainingCost
print("Within Set Sum of Squared Errors (WSSSE):", wssse)

#--------------------------------------------------- DELTA CONVERSION

if DeltaTable.isDeltaTable(spark, '/tmp/tweet-delta'):
    print('Table exist. Updating existing table')

    deltaTable = DeltaTable.forPath(spark, '/tmp/tweet-delta')

    deltaTable.alias('original').merge(predictions.alias('updated'), 'original.filtered_words = updated.filtered_words').whenMatchedUpdate(set = {'filtered_words' : predictions.filtered_words, 'word_vectors' : predictions.word_vectors, 'prediction' : predictions.prediction}).whenNotMatchedInsert(values = {'filtered_words' : predictions.filtered_words, 'word_vectors' : predictions.word_vectors, 'prediction' : predictions.prediction}).execute()
else:
    print('Table doesnt exist. Creating new table')
    predictions.write.format('delta').save('/tmp/tweet-delta')

deltaDf = spark.read.format('delta').load('/tmp/tweet-delta')
print('Show delta table')
deltaDf.show()
