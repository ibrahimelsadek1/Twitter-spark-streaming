#!/bin/bash
	
export SPARK_HOME=/opt/spark2
export HADOOP_CONF_DIR=/opt/hadoop

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 1 \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --queue streaming \
    --conf spark.pyspark.python=/usr/bin/python3 \
     /home/itversity/fact_processing.py
