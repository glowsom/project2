#!/bin/bash
root=/home/spark/p2
pRoot=/user/spark/p2
#batchid=`cat $localRoot/logs/current_batch.txt`
#LOGFILE=$localRoot/logs/log_batch_$batchid

cd $root/jars

echo "Running script for Data Filtering and Enrichment..." >> $LOGFILE

spark-submit \
--packages com.databricks:spark-xml_2.11:0.4.1 \
--jars hbase-annotations.jar,hbase-common.jar,hbase-server.jar,hbase-client.jar,hbase-protocol.jar,hbase-hadoop2-compat.jar,zookeeper.jar,htrace-core-3.2.0-incubating.jar \
--class music.DataFilterEnrich \
--master yarn \
$root/proj2/target/scala-2.11/proj2_2.11-0.1.jar \
$pRoot/conf3.txt \
$pRoot/Data/Web/file.xml \
$pRoot/Data/Mob/file.txt \
$pRoot/enrichedDF.parquet

echo "Data Filtering and Enrichment Done..." >> $LOGFILE
