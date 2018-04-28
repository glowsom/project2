# project2

Project takes music data and filters, enriches, then analyzes it to answer some questions.

Entire project is run by music_cron.sh which is in the scripts folder.

Creates a batch file which is used to creates log files which track the activities/processes in the application.

There are lookup Tables whihc are stored in HBase for quick access, which help with filtering, enriching and analyzing the data.

By reading through music_cron.sh you'll understand how the application works.

# Problems to be solved.

1. Determine top 10 station_id(s) where maximum number of songs were played, which were liked by unique users.

2. Determine total duration of songs played by each type of user, where type of user can be 'subscribed' or 'unsubscribed'. An unsubscribed user is the one whose record is either not present in Subscribed_users lookup table or has subscription_end_date earlier than the timestamp of the song played by him.

3. Determine top 10 connected artists. Connected artists are those whose songs are most listened by the unique users who follow them.

4. Determine top 10 songs who have generated the maximum revenue. Royalty applies to a song only if it was liked or was completed successfully or both. Song was completed successfully if song_end_type = 1.

5. Determine top 10 unsubscribed users who listened to the songs for the longest duration.

The results to these problems as solved by the project are in their respectively named folders here: HDFS_FILES/user/spark/p2/output/.

#Important Notice
The problems involving Timestamps yeild illogical results.
This is due to the fact that many of the records contain timestamps that are illogical.
According to the data many of the songs end about a year before they start. Others also end about a year after.
Many bad records (based on logic) were retained because the project's filtering parameters allowed them to remain.

Data/Web and Data/Mob are the folders that contain the data on which was analyzed by this project.

SBT packaged jar file used for running all spark programs is proj2_2.11-0.1.jar.
