#!/bin/bash

export SPARK_HOME=/opt/spark2
export HADOOP_CONF_DIR=/opt/hadoop

$SPARK_HOME/bin/spark-sql \
    --conf spark.sql.shuffle.partitions=4 \
    --queue streaming \
    -f /home/itversity/hive_script.sql