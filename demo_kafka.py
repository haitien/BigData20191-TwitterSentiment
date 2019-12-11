from __future__ import print_function
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

if __name__ == "__main__":
  # ssc = StreamingContext(sc, 1)
  
  # zkQuorum = "localhost:2181"
  # topic = "test"
  # kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
  # lines = kvs.map(lambda x: x[1])
  # counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
  # counts.pprint()
  
  # ssc.start()
  # ssc.awaitTermination()
  def binaryToString(s):
    return s.decode('utf-8')
  
  toString = udf(lambda s: binaryToString(s))
  
  spark = SparkSession.builder.appName("DemoWithKafka").getOrCreate()
  df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:2181")\
    .option("subscribe", "test")\
    .load()
  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  df.withColumn("value", toString(df["value"]))
  lines = df.withColumnRenamed("value", "text")
  print(type(lines))
  lines.printSchema()
  model = PipelineModel.read().load(
    "/home/haitien/Desktop/TwitterSentimentAnalysis_BigData20191/scripts/saved_model/model")
  prediction = model.transform(lines)
  selected = prediction.select("text", "probability", "prediction")
  selected.writeStream.format("console").option("truncate", "false").start()
  selected.awaitTermination()