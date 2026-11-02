#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
  start-master.sh -p 7077
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
elif [ "$SPARK_WORKLOAD" == "streaming" ]
then
  exec spark-submit \
    --conf spark.cores.max=4 \
    --conf spark.executor.memory=1g \
    --conf spark.sql.shuffle.partitions=4 \
    /opt/spark/apps/streaming/consumidor_pedidos.py
fi
