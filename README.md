# Hadoop Spark Project 2

Project takes music data, then filters, enriches, and analyzes it to answer some questions.

Entire project is run by music_cron.sh which is in the scripts folder.

Creates a batch file which is used to creates log files which track the activities/processes in the application.

There are lookup Tables whihc are stored in HBase for quick access, which help with filtering, enriching and analyzing the data.

The requirements and guidelines for this project are contained in Project2_requirements.pdf.

# Results

The results to given problems as solved by this project are in their respectively named folders here: HDFS_FILES/user/spark/p2/output/.

The scala code is contained in the project folder named proj2; it follows the sbt folder structure.

SBT packaged jar file used for running all spark programs is proj2_2.11-0.1.jar.

# Important Notice
The problems involving Timestamps yeild illogical results.
This is due to the fact that many of the records contain timestamps that are illogical.
According to the data many of the songs end about a year before they start. Others also end about a year after.
Many bad records (based on logic) were retained because the project's filtering parameters allowed them to remain.

Data/Web and Data/Mob are the folders that contain the data on which was analyzed by this project.

# Scheduler
Part of the project is to shedule this application to run every 24 hours.
Crontab has been used to run the application every morning at 8am.

# Logs
Whenever the application is run, a log file is created in the logs folder to help track its activities.
