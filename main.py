from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
#
# from pyspark import SparkContext
# from pyspark.sql.context import SQLContext
# from pyspark.streaming import StreamingContext
#
# TRAIN_DATA_PATH = '/home/haitien/Desktop/TwitterSematic_BigData20191/data/training.1600000.processed.noemoticon.csv'
# TEST_DATA_PATH = '/home/haitien/Desktop/TwitterSematic_BigData20191/data/testdata.manual.2009.06.14.csv'

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession

if __name__ == '__main__':
  
  # Create a local StreamingContext with two working thread and batch interval of 1 second
  # sc = SparkContext("local[*]", "NetworkWordCount")
  # sqlContext = SQLContext(sc)
  # # print("hello")
  # # df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', inferschema='true').load(
  # #         TRAIN_DATA_PATH)
  # # df.show(40)
  #
  # spark = SparkSession.builder \
  #         .master("local") \
  #         .appName("Word Count") \
  #         .config("spark.some.config.option", "some-value") \
  #         .getOrCreate()
  # # Prepare training documents, which are labeled.
  #
  
  from pyspark import SparkContext
  from pyspark.streaming import StreamingContext
  from pyspark.streaming.kafka import KafkaUtils
  
  sc = SparkContext(appName="PythonStreamingKafkaWordCount")
  ssc = StreamingContext(sc, 2)
  
  zkQuorum = "localhost:9092"
  topic = "test"
  kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
  lines = kvs.map(lambda x: x[1])
  counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
  counts.pprint()
  
  ssc.start()
  ssc.awaitTermination()
