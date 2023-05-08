
# Twitter Spark Streaming Project

The Twitter Spark Streaming Project is a big data processing application that utilizes Apache Spark and Spark Streaming to extract meaningful insights and analyze real-time data streams from Twitter. The project is designed to process large volumes of tweets in real-time and analyze them for various purposes such as sentiment analysis, trend analysis, and user behavior analysis.

This project is intended for data engineers, data scientists, and developers who are interested in learning how to use Apache Spark and Spark Streaming to process real-time data streams from Twitter.

Architecture and Design

The project consists of four main components: the Twitter listener, the Spark Streaming script, the Hive dimensions script, and the SparkSQL fact table script. The Twitter listener extracts data from the Twitter API every 5 minutes and sends it to the Spark Streaming script as a JSON file for every batch. The Spark Streaming script applies a user-defined schema to the received JSON file and writes the data to HDFS as a Parquet file partitioned by year, month, day, and hour. The Hive dimensions script creates three tables and implements a Slowly Changing Dimension in the users_raw table. Finally, the SparkSQL fact table script extracts data from the dimensions tables using SparkSQL with a hive metastore and generates a new attribute on the fly using SQL. Hashtags are also extracted to make them as dim on the fly and to know popular hashtags.

Usage

To use the Twitter Spark Streaming Project, you will need to have access to the Twitter API and have a Spark cluster set up. You will also need to modify the code to suit your specific use case.

To start using the project, you can follow the steps below:

Clone the repository to your local machine.
Modify the twitter_listener.py script to extract data every 5 minutes and start and end data are (day-1 with period 5 minutes between start and end).
Modify the spark_streaming.py script to apply a user-defined schema and write the data to HDFS as a Parquet file partitioned by year, month, day, and hour.
Modify the hive_script.sql script to create three tables and implement a Slowly Changing Dimension in the users_raw table.
Modify the fact_processing.py script to extract data from the dimensions tables using SparkSQL with a hive metastore and generate a new attribute on the fly using SQL.
Once you have made the necessary modifications to the code, you can run the scripts on your Spark cluster to start processing real-time data streams from Twitter.

Conclusion

The Twitter Spark Streaming Project is a powerful big data processing application that can be used for a wide range of purposes such as sentiment analysis, trend analysis, and user behavior analysis. By utilizing Apache Spark and Spark Streaming, this project can process large volumes of tweets in real-time and extract meaningful insights from them.
