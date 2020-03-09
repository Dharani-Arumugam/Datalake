# Data Lake

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, task is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Datasets :
*Song Dataset :* collection of JSON files that describes the songs such as title, artist name, year, etc.
Song data: `s3://udacity-dend/song_data`

*Log Dataset :* collection of JSON files where each file covers the users activities over a given day.
Log data: `s3://udacity-dend/log_data`

### Star Schema :
Create a star schema optimized for queries on song play analysis. This includes the following tables :

*Fact Table :*
- songplays - records in event data associated with song plays i.e. records with page `NextSong`
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

*Dimension Tables :*
  - users - users in the app
*user_id, first_name, last_name, gender, level*
  - songs - songs in music database
*song_id, title, artist_id, year, duration*
  - artists - artists in music database
*artist_id, name, location, latitude, longitude*
  - time - timestamps of records in songplays broken down into specific units
*start_time, hour, day, week, month, year, weekday*

### ETL pipeline :

 - Load from data from s3 buckets to spark data frame using pysparksql/ data frame
 - Data from spark data frames are processed to move to a parquet files stored in local environment/or in s3 buckets

  ### Run the scripts:

1. `etl_local.py` - builds the etl process to load the data from s3 and process it as parquet files(columnar storage) in local environment

2. `etl_aws.py`  - builds the etl process to load the data from s3 bucket and process it as parquet files and store it again in the output s3 bucket.
