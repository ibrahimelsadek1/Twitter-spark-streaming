# Twitter-spark-streaming

Apache Spark is a popular open-source big data processing framework that provides high-performance,
scalable, and fault-tolerant processing of large data sets. Spark Streaming is a real-time processing
extension to Spark that enables the processing of live data streams. These technologies have become
the backbone of many modern big data applications, including real-time analytics, machine learning, and
data processing.
In this report, we present a project that utilizes Spark and Spark Streaming to process data from the
Twitter API. The goal of this project is to extract meaningful insights and analyze real-time data streams
from Twitter. The project uses Spark's distributed processing capabilities and Spark Streaming's realtime processing capabilities to analyze large volumes of tweets in real-time.
The report provides an overview of the project's architecture, design, and implementation details.



Code details:
1- Twitter Listener: twitter_listener.py
 Modifications on code:
- Script extract data every 5 minutes
- Start and End data are (day-1 with period 5 minutes between start and end)
- Data is being sent to spark streaming as a JSON file for every batch.
- Script is running all time the machine is running
 Source : twitter API
 Target : socket stream to spark streaming.

2- Spark streaming script: spark_streaming.py

 Code specs:
- User defined schema that applied on the received JSON file
- Data received is being written on HDFS as Parquet file with partitioned by (year , month
, day , hour)
- Script is running all time the machine is running
 Source : socket stream from listener
 Target : HDFS: /twitter-landing-data.

3- Hive dimensions Script: hive_script.sql
 Code specs:

- Create statement for three tables (twitter_landing_table , users_raw , tweets_raw)
- Slowly Changing Dimension in Users_raw Table
- To achieve a Slowly Changing Dimension (SCD) in the users_raw table, a work-around
approach was adopted. The concept involves creating a temporary table to hold the data
from the already existing users_raw table and new users from the twitter_landing_table
who are not in users_raw.
- The temporary table serves as a merge table where the new data is added and the existing
data is updated based on the user_id column. Once the merge table is updated, all data in
the users_raw table is replaced with the data from the merge table.
 Source for twitter_landing_table : HDFS: /twitter-landing-data
 Source for dimensions : table(twitter_landing_table)
 Target : 2 Tables (tweets_raw , users_raw) located at HDFS: /twitter-raw-data


4- SparkSQL Fact Table Script: fact_processing.py
 Code specs:
- Extracting data from dims using SparkSQL with hive metastore
- New attribute (Trust_Ratio_Perc) has been generated on the fly using some SQL
- Hashtags was extracted alone with tweet_id to make it as dim on the fly to know
popular hashtags
- Hashtags like: layoff, #layoffs #job #loss has been excluded from results because it refers
to the keyword that we already searched for.
 Source: dimensions Tables (tweets_raw , users_raw)
 Target : Tables (twitter_fact_processed) located at HDFS: /twitter-processed-data
