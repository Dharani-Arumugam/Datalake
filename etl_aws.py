import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']     = config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['KEYS']['AWS_SECRET_ACCESS_KEY']

input_data  = "s3a://udacity-dend/"
output_data = "s3a://dharani-data-lake/"

def create_spark_session():
    '''
    Creates a Spark session
    Arguments : None
    '''
    spark = SparkSession\
            .builder\
            .appName("Spark on AWS")\
            .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
     '''
    Process the song data from s3 bucket to the dataframes
    Arguments : spark           - session variable
                input_data      - input location in s3 Bucket
                output_data     - output data location in s3 Bucket to store
                                  parquet files
    '''
    #Process song table
    songdata_path = os.path.join(input_data, "song-data/A/A/A/*.json")

    df_songdata = spark.read.json(songdata_path)
    df_songdata.printSchema()

    songs_df = df_songdata['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_df = songs_df.dropDuplicates(['song_id'])
    songs_df.printSchema()
    songs_df.show(5, truncate=False)

    song_output = output_data + "songs.parquet"
    songs_df.write.mode("overwrite").partitionBy("year","artist_id").parquet(song_output)

    #Process artist table

    artists_df = df_songdata['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_df = artists_df.dropDuplicates(['artists_id'])
    artists_df.printSchema()
    artists_df.show(5, truncate = False)

    artists_output = output_data + "artists.parquet"
    artists_df.write.mode("overwrite").parquet(artists_output)

def process_log_data(spark, input_data, output_data):
    '''
    Process the log data from s3 bucket to the dataframes
    Arguments : spark           - session variable
                log_data_local  - logdata location in s3 Bucket
                output_data     - store the output parquet files
    '''


    logdata_path = os.path.join(input_data, "log-data/*/*/*.json")

    df_logdata = spark.read.json(logdata_path)
    df_logdata.printSchema()
    df_logdata_filter = df_logdata.filter(df_logdata.page=='NextSong')

    #Process user table
    users_df = df_logdata_filter['userId', 'firstName', 'lastName', 'gender', 'level']
    users_df = users_df.dropDuplicates(['userId'])
    users_df.printSchema()
    users_df.show(5, truncate= False)

    users_output = output_data + "users.parquet"
    users_df.write.mode("overwrite").parquet(users_output)

    #Process time table
    #create user defined functions to convert 'ts'
    @udf(t.TimestampType())
    def get_timestamp(ts):
        return datetime.fromtimestamp(ts/1000.0)
    df_logdata_filter = df_logdata_filter.withColumn('timestamp', get_timestamp("ts"))

    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0).strftime('%Y-%m-%d %H:%M:%S')

    df_logdata_filter = df_logdata_filter.withColumn('datetime',get_datetime("ts"))

    df_logdata_filter.printSchema()
    df_logdata_filter.show(5truncate= False)
    # extract columns to create time table
    time_df = df_logdata_filter.select(
                 col('datetime').alias('start_time')
                ,hour('datetime').alias('hour')
                ,dayofmonth('datetime').alias('day')
                ,weekofyear('datetime').alias('week')
                ,month('datetime').alias('month')
                ,year('datetime').alias('year')
               )
    time_df = time_df.dropDuplicates(['start_time'])
    time_df.printSchema()
    time_df.show(5, truncate=False)

    time_output = output_data + "time.parquet"
    time_df.write.mode("overwrite").partitionBy("year","month").parquet(time_output)

    songdata_path = os.path.join(input_data, "song-data/A/A/A/*.json")
    df_songdata = spark.read.json(songdata_path)

    df_logdata_filter = df_logdata_filter.join(df_songdata, df_songdata.title == df_logdata_filter.song )

    songplays_df = df_logdata_filter.select(
                 col('ts').alias('ts')
                ,col('userId').alias('user_id')
                ,col('level').alias('level')
                ,col('song_id').alias('song_id')
                ,col('artist_id').alias('artist_id')
                ,col('ssessionId').alias('session_id')
                ,col('location').alias('location')
                ,col('userAgent').alias('user_agent')
                ,col('year').alias('year')
                ,month('datetime').alias('month'))

    songplays_df = songplays_df.selectExpr("ts as start_time")
    songplays_df.select(monotonically_increasing_id().alias('songplay_id')).collect()

    songplays_output = output_data + "songplays.parquet"
    songplays_df.write.mode("overwrite").partitionBy("year","month").parquet(songplays_output)

def main():
    '''
    Main function to create spark session and process song data and log data
    Arguments : None
    '''
    spark                  =  create_spark_session()
    songs, artists         =  process_song_data(spark, input_data, output_data)
    users, time, songplays =  process_log_data (spark, input_data, output_data)
    print("ETL processing on AWS cluster is done")

if __name__ == "__main__":
    main()
