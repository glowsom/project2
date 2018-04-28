#!/bin/bash
#root=file:/home/samobenga/Jars
root=file:~/work/p2/
projectRoot=hdfs://nn01.itversity.com:8020/user/samobenga
#batchid=`cat ~/logs/current_batch.txt`
#LOGFILE=~/project2/logs/log_batch_$batchid

#echo "Running script for data analysis..." >> $LOGFILE

cd /usr/hdp/current/spark2-client/
export SPARK_MAJOR_VERSION=2

rm ~/Jars/project2_2.11-0.1.jar
cd ~/project2
sbt package
mv ~/project2/target/scala-2.11/project2_2.11-0.1.jar ~/Jars

cd ~/Jars

spark-submit \
--conf spark.ui.port=10181 \
--class music.Hbase_LookUp \
--master yarn \
#--jars $projectRoot/Jars/hbase-annotations.jar,$projectRoot/Jars/hbase-common.jar,$projectRoot/Jars/hbase-server.jar,$projectRoot/Jars/hbase-client.jar,$projectRoot/Jars/hbase-protocol.jar,$projectRoot/Jars/hbase-hadoop2-compat.jar,$projectRoot/Jars/zookeeper.jar,$projectRoot/Jars/htrace-core-3.2.0-incubating.jar \
--jars $root/jars/hbase-annotations.jar,$root/jars/hbase-common.jar,$root/jars/hbase-server.jar,$root/jars/hbase-client.jar,$root/jars/hbase-protocol.jar,$root/jars/hbase-hadoop2-compat.jar,$root/jars/zookeeper.jar,$root/jars/htrace-core-3.2.0-incubating.jar \
$root/jars/project2_2.11-0.1.jar \
$projectRoot/output/problem


