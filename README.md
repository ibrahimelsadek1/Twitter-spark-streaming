# Twitter Spark Streaming Project



This project utilizes Apache Spark and Spark Streaming to process real-time data streams from the Twitter API, extracting meaningful insights and analyzing large volumes of tweets in real-time. The project includes the following components:

**Twitter Listener(twitter_listener.py):**


A Python script that extracts data from the Twitter API every 5 minutes and sends it as a JSON file to a socket stream connected to Spark Streaming.

**Spark Streaming Script (spark_streaming.py):**

A Python script that receives the JSON files from the socket stream and writes them as Parquet files on HDFS, partitioned by year, month, day, and hour.

**Hive Dimensions Script (hive_script.sql):**

A SQL script that creates three tables (twitter_landing_table, users_raw, tweets_raw) and implements a Slowly Changing Dimension (SCD) in the users_raw table to merge new data with existing data based on the user_id column.

**SparkSQL Fact Table Script (fact_processing.py:**

A Python script that extracts data from the dimensions tables using SparkSQL with Hive Metastore, generates a new attribute (Trust_Ratio_Perc) on the fly using SQL, extracts popular hashtags as a dimension, and writes the processed data as a table on HDFS.

# Requirements

- Apache Spark 2.4+
- Hadoop 2.7+
- Python 3.6+
- PySpark 2.4+ (for Spark Streaming)
- Hive 2.3+ (for Hive Metastore)

# Usage

- Clone the repository to your local machine.
- Modify the twitter_listener.py script with your Twitter API credentials and run it to start extracting data from the Twitter API.
- Modify the spark_streaming.py script with your HDFS path and run it to start processing the data streams.
- Modify the hive_script.sql script with your database and table names and run it to create the required tables and implement the SCD.
- Modify the fact_processing.py script with your HDFS paths and run it to generate the processed data table.

