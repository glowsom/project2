#!/bin/bash

root=/home/spark/p2
batchid=`cat $root/logs/current_batch.txt`
LOGFILE=$root/logs/log_batch_$batchid

#Loading Lookup data into hbase tables for quicker reads.

echo "Creating LookUp Tables in HBase..." >> $LOGFILE


echo "Populating song_artist_map Table from song-artist.txt..." >> $LOGFILE

echo "create 'song_artist_map', 'artist'" | hbase shell
echo "create 'stn_geocd_map', 'geo'" | hbase shell
echo "create 'user_artist_subscn_map', 'artist', 'subscn'" | hbase shell


echo "Populating song_artist_map Table from song-artist.txt..." >> $LOGFILE

file="$root/Data/LookUp/song-artist.txt"
while IFS= read -r line
do
	song_id=`echo $line | cut -d',' -f1`
	artist_id=`echo $line | cut -d',' -f2`
	echo "put 'song_artist_map', '$song_id', 'artist:id', '$artist_id'" | hbase shell
done <"$file"


echo "Populating stn_geocd_map Table from stn-geocd.txt..." >> $LOGFILE

file="$root/Data/LookUp/stn-geocd.txt"
while IFS= read -r line
do
	stn_id=`echo $line | cut -d',' -f1`
	geo_cd=`echo $line | cut -d',' -f2`
	echo "put 'stn_geocd_map', '$stn_id', 'geo:cd', '$geo_cd'" | hbase shell
done <"$file"


echo "Populating artist col-family in user_artist_subscn_map Table from user-artist.txt..." >> $LOGFILE

file="$root/Data/LookUp/user-artist.txt"
while IFS= read -r line
do
	user_id=`echo $line | cut -d',' -f1`
	artist_id=`echo $line | cut -d',' -f2`
	echo "put 'user_artist_subscn_map', '$user_id', 'artist:id', '$artist_id'" | hbase shell
done <"$file"


echo "Populating subscn col-family in user_artist_subscn_map Table from user-subscn.txt..." >> $LOGFILE

file="$root/Data/LookUp/user-subscn.txt"
while IFS= read -r line
do
	user_id=`echo $line | cut -d',' -f1`
	start=`echo $line | cut -d',' -f2`
	end=`echo $line | cut -d',' -f3`
	echo "put 'user_artist_subscn_map', '$user_id', 'subscn:start', '$start'" | hbase shell
	echo "put 'user_artist_subscn_map', '$user_id', 'subscn:end', '$end'" | hbase shell
done <"$file"

echo "All LookUp Tables Completed..." >> $LOGFILE


