#!/bin/bash
root=/home/spark/p2
pRoot=/user/spark/p2


echo "Running Music job..."


#Create/update Batch info

if [ -f "$root/logs/current_batch.txt" ]
then
	echo "Batch file found & updating..."

	#Update batchid & current batch file to reflect current number of iterations
	batchid=`cat $root/logs/current_batch.txt`
        let "batchid++"
        echo -n "$batchid" > "$root/logs/current_batch.txt"

	LOGFILE=$root/logs/log_batch_$batchid

	echo "Music job starting" >> $LOGFILE

	echo "Deleting HDFS Data from previous day" >> $LOGFILE
	hdfs dfs -rm -r $pRoot/Data/Web/file.xml
	hdfs dfs -rm -r $pRoot/Data/Mob/file.txt
	hdfs dfs -rm -r $pRoot/output
	hdfs dfs -rm -r $pRoot/enrichedDF.parquet	

else
	#All First time activities
	
	echo "Creating current batch file..."

	#Initializing batchid and batch file which will be used to track the log files
	batchid="1"	
	echo -n $batchid > "$root/logs/current_batch.txt"
	chmod 775 $root/logs/current_batch.txt

	LOGFILE=$root/logs/log_batch_$batchid

	echo "Music Job Starting on" >> $LOGFILE

	echo "Creating Directories for data injestion" >> $LOGFILE
        hdfs dfs -mkdir $pRoot
	hdfs dfs -chmod g+w $pRoot
	hdfs dfs -mkdir $pRoot/Data
	hdfs dfs -mkdir $pRoot/Data/Mob
	hdfs dfs -mkdir $pRoot/Data/Web

	#Store Lookup data into HBASE TABLE
	sh $root/scripts/populate-lookup.sh

	#Load HBase config table for hbase access
	hdfs dfs -put $root/conf3.txt $pRoot

	#This wil be the scheduler for this application. It will run at 8am every morning.
	#write out current crontab
	crontab -l > music_cron
	#echo new cron into cron file. This will run music_cron.sh every morning at 8am.
	echo "00 08 * * * root $root/scripts/music_cron.sh" >> music_cron

	echo "install new cron file" >> $LOGFILE
	crontab music_cron
	rm music_cron
fi



echo "Injesting Data..."
sh $root/scripts/injest_data.sh

echo "Filtering & Enriching Data..."
sh $root/scripts/data_filter_enrichment.sh

echo "Analysing Data..."
sh $root/scripts/data_analysis.sh

echo "Music Job Completed... Scheduled to re-run at 8am tomorrow." >> $LOGFILE
echo "Result files Located in HDFS at directory: /user/spark/p2/output..." >> $LOGFILE

echo "Music job is done..."
