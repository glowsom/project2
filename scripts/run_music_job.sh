#!/bin/bash
root=/home/spark/p2
pRoot=/user/spark/p2

#If batch file doesn't exist, one will be created and initialized to 1

if [ -f "$root/logs/current_batch.txt" ]
then
	echo "Batch File Found!"
	#Update batch file
	batchid=`cat $root/logs/current_batch.txt`
	echo -n $batchid > "$root/logs/current_batch.txt"

	#Delete Data from previous day
	hdfs dfs -rm -r $pRoot/Data/Web/file.xml
	hdfs dfs -rm -r $pRoot/Data/Mob/file.txt
	#Delete and remake output directory
	hdfs dfs -rm -r $pRoot/output

else
	#All First time activities
	#Create current batch file
	echo -n "1" > "$root/logs/current_batch.txt"
	hdfs dfs -put $root/Data/ $pRoot
	hdfs dfs -chmod g+w $pRoot/Data
	sh ~/work/p2/scripts/populate-lookup.sh
	batchid=`cat $root/logs/current_batch.txt`
fi

#Make sure of batch file accessibility
chmod 775 $root/logs/current_batch.txt

LOGFILE=$root/logs/log_batch_$batchid


echo "Filtering and Enriching Data..." >> $LOGFILE

sh $root/scripts/data_filter_enrichment.sh

echo "Analysing Data..." >> $LOGFILE

sh $root/scripts/data_analysis.sh


