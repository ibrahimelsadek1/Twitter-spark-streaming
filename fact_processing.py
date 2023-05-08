

import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# May take awhile locally
spark = SparkSession.builder.appName("Fact table job").enableHiveSupport().getOrCreate()



raw_data=spark.sql("select t.year,t.month,t.day,t.hour,count(distinct t.user_id) num_of_user  ,sum(case when u.verified=True then 1 else 0 end) num_of_verified_users, round(sum(case when u.verified=True then 1 else 0 end)/count(distinct t.user_id) *100) Trust_Ratio_Perc, count(tweet_id) num_of_tweets, sum(impression_count) total_impression , sum(retweets) total_retweets_num from project.tweets_raw t join project.users_raw u on t.user_id=u.user_id group by t.year,t.month,t.day,t.hour")
tweets=spark.sql(" select * from project.tweets_raw")

def extract_hashtags(text):
    hashtags = re.findall(r"#\w+", text)
    return hashtags

extract_hashtags_udf = udf(extract_hashtags, ArrayType(StringType()))
hashtages = tweets.withColumn("hashtags", explode(extract_hashtags_udf("text"))).select("year","month","day","hour","tweet_id","hashtags")
hashtages.createOrReplaceTempView("tempview")

extracted_hashtages=spark.sql(" select year jyear,month jmonth,day jday,hour jhour, lower(hashtags) hashtag , count(lower(hashtags)) as num , row_number() over(order by count(lower(hashtags)) desc) rnk  from tempview group by year,month,day,hour , lower(hashtags) ")
extracted_hashtages=extracted_hashtages.select("jyear","jmonth","jday","jhour","hashtag").filter("rnk = 1")

cond = [((raw_data.year == extracted_hashtages.jyear) & (raw_data.month == extracted_hashtages.jmonth) & (raw_data.day == extracted_hashtages.jday) & (raw_data.hour == extracted_hashtages.jhour))]

joined_df = raw_data.join(extracted_hashtages, on=cond,how="left").drop("jyear","jmonth","jday","jhour").withColumnRenamed('hashtag','most_popular_hashtag')


# Save the DataFrame as a table in Spark
joined_df.write.mode("overwrite").option("path","hdfs://localhost:9000/twitter-processed-data/fact_table_processed").saveAsTable("project.fact_table_processed")


