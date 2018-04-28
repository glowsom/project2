#!/bin/bash
root=/home/spark/p2
pRoot=/user/spark/p2
batchid=`cat $root/logs/current_batch.txt`
LOGFILE=$root/logs/log_batch_$batchid


echo "Injesting Mobile & Web Data..." >> $LOGFILE

#Move Mobile and Web data into HDFS
hdfs dfs -put $root/Data/Web/file.xml $pRoot/Data/Web
hdfs dfs -put $root/Data/Mob/file.txt $pRoot/Data/Mob

#Make output directory for results
hdfs dfs -mkdir $pRoot/output


echo "Mobile & Web Data Injestion Completed..." >> $LOGFILE
