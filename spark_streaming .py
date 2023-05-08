# Import your dependecies
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


warehouse_location = ('/user/itversity/warehouse')
spark = SparkSession \
    .builder \
    .appName("TwitterST") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


# spark.conf.set("spark.executor.instances", 10)
# spark.conf.set("hive.exec.dynamic.partition", "true")
# spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
# spark.conf.set("spark.sql.streaming.concurrentJobs" , 1 )
# spark



# read the tweet data from socket
tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7777) \
    .load()
       
    

schema = StructType([
    StructField("tweet_id", StringType()),
    StructField("tweet_date", StringType()),
    StructField("text", StringType()),
    StructField("retweets", IntegerType()),
    StructField("impression_count", IntegerType()),
    StructField("username", StringType()),
    StructField("user_id", StringType()),
    StructField("verified", BooleanType()),
    StructField("followers", IntegerType()),
    StructField("location", StringType())
])

parsed = tweet_df.select(from_json(tweet_df.value, schema).alias("record"))

df = parsed.select("record.*")\
                .withColumn("year", year(to_timestamp("tweet_date", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))\
                .withColumn("month", month(to_timestamp("tweet_date", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))\
                .withColumn("day", dayofmonth(to_timestamp("tweet_date", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))\
                .withColumn("hour", hour(to_timestamp("tweet_date", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))\
                           
                

writeTweet = df.writeStream\
                      .outputMode("append")\
                      .partitionBy("year", "month","day","hour")\
                      .format("parquet")\
                      .option("path", "/twitter-landing-data")\
                      .option("checkpointLocation", "/data/checkpoint")\
                      .start()




writeTweet.awaitTermination()