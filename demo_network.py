"""
 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/network_wordcount.py localhost 9999`
"""
from __future__ import print_function
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

if __name__ == "__main__":
  spark = SparkSession.builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

  lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

  lines = lines.withColumnRenamed("value", "text")
  lines.printSchema()
  model = PipelineModel.read().load("/home/haitien/Desktop/TwitterSentimentAnalysis_BigData20191/scripts/saved_model/model")
  prediction = model.transform(lines)
  selected = prediction.select("text", "probability", "prediction")
  query = selected.writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

  query.awaitTermination()
  
  #
  # lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
  # counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
  # counts.pprint()
  
  # ssc.start()
  # ssc.awaitTermination()
