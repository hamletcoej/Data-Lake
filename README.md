# SPARKIFY

## Motivation

Sparkify has grown their user base and song database even more and want to move their data warehouse to a data lake.


## Tech used

Built with:
- S3 storage
- Python
- Spark


## Arcitechure

Sparkify data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

An ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

Tables required:
stagingevents - log data from s3://udacity-dend/log_data
stagingsongs - song data from s3://udacity-dend/song_data

Fact Table
songplays - records in event data associated with song plays i.e. records with page NextSong

Dimension Tables
users - users in the app
songs - songs in music database
artists - artists in music database
time - timestamps of records in songplays broken down into specific units


# Running the Process

To run it manually you can launch etl.py process from the command line by typing 'python etl.py'.
This process could be set up to execute on a task scheduler. 


## Script Files

- etl.py
This script loads song and log data from S3, process the data into analytics tables using Spark, and load them back into another S3 bucket.
