#!/bin/bash
	
export SPARK_HOME=/opt/spark2
export HADOOP_CONF_DIR=/opt/hadoop

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --queue streaming \
    --conf spark.pyspark.python=/usr/bin/python3 \
     /home/itversity/fact_processing.py
