#!/bin/bash
root=/home/spark/p2
pRoot=/user/spark/p2
batchid=`cat $root/logs/current_batch.txt`
LOGFILE=$root/logs/log_batch_$batchid

cd $root/jars

echo "Running script for data analysis..." >> $LOGFILE

spark-submit \
--jars hbase-annotations.jar,hbase-common.jar,hbase-server.jar,hbase-client.jar,hbase-protocol.jar,hbase-hadoop2-compat.jar,zookeeper.jar,htrace-core-3.2.0-incubating.jar \
--class music.DataAnalysis \
--master yarn \
$root/proj2/target/scala-2.11/proj2_2.11-0.1.jar \
$pRoot/conf3.txt \
$pRoot/output/problem1 \
$pRoot/output/problem2 \
$pRoot/output/problem3 \
$pRoot/output/problem4 \
$pRoot/output/problem5 \
$pRoot/enrichedDF.parquet

echo "Data analysis done..." >> $LOGFILE
