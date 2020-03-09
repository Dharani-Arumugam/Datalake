import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')


log_data_local  = 'data/log-data'
output_data     = 'data/output-data/'
song_data_local = 'data/song-data'

def create_spark_session():
    '''
    Creates a Spark session
    Arguments : None
    '''
    spark = SparkSession\
            .builder\
            .master("local")\
            .appName("Spark local")\
            .getOrCreate()
    return spark

def process_song_data(  spark
                       ,song_data_local
                       ,output_data):
    '''
    Process the song data from s3 bucket to the dataframes using pysparksql
    Arguments : spark           - session variable
                song_data_local - songdata location in s3 Bucket
                output_data     - store the output parquet files
    '''
    df_songdata = spark.read.json(song_data_local)
    df_songdata.printSchema()
    df_songdata.createOrReplaceTempView("songs")

    songs_df = spark.sql("""
                         SELECT  song_id
                                ,title
                                ,artist_id
                                ,year
                                ,duration
                        FROM     songs
                        GROUP BY song_id
                                ,title
                                ,artist_id
                                ,year
                                ,duration
                        ORDER BY song_id
                    """)
    songs_df.printSchema()
    songs_df.show(5, truncate=False)
    song_table_output = output_data +"songs.parquet"
    songs_df.write.mode("overwrite").partitionBy("year","artist_id").parquet(song_table_output)

    df_songdata.createOrReplaceTempView("artists")
    artists_df = spark.sql("""
                        SELECT   artist_id
                                ,artist_name
                                ,artist_location
                                ,artist_latitude
                                ,artist_longitude
                        FROM    artists
                        WHERE   artist_id IS NOT NULL
                        GROUP BY  artist_id
                                 ,artist_name
                                 ,artist_location
                                 ,artist_latitude
                                 ,artist_longitude
                        ORDER BY artist_id
                        """)
    artists_df.printSchema()
    artists_df.show(5, truncate=False)
    artists_table_output = output_data+"artists.parquet"
    artists_df.write.mode("overwrite").parquet(artists_table_output)

    return songs_df, artists_df

def process_log_data(  spark ,song_data_local,log_data_local ,output_data):
    '''
    Process the log data from s3 bucket to the dataframes using pysparksql
    Arguments : spark           - session variable
                log_data_local  - logdata location in s3 Bucket
                output_data     - store the output parquet files
    '''
    df_logdata = spark.read.json(log_data_local)
    df_logdata.printSchema()

    df_logdata_filter = df_logdata.filter(df_logdata.page=='NextSong')
    df_logdata_filter.createOrReplaceTempView("users")
    users_df = spark.sql("""
                    SELECT  userId
                           ,firstName
                           ,lastName
                           ,gender
                           ,level
                    FROM   users
                    WHERE  userId IS NOT NULL
                    GROUP BY userId
                            ,firstName
                            ,lastName
                            ,gender
                            ,level
                    ORDER BY userId
                    """)
    users_df.printSchema()
    users_df.show(5, truncate=False)
    users_table_output = output_data+"users.parquet"
    users_df.write.mode("overwrite").parquet(users_table_output)
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

    df_logdata_filter.show(5)

    df_logdata_filter.createOrReplaceTempView("times")
    time_df = spark.sql("""
                    SELECT  DISTINCT datetime       AS start_time
                             ,hour(timestamp)       AS hour
                             ,day(timestamp)        AS day
                             ,weekofyear(timestamp) AS week
                             ,month(timestamp)      AS month
                             ,year(timestamp)       AS year
                             ,dayofweek(timestamp)  AS weekday
                    FROM times
                    ORDER BY start_time
                        """)
    time_df.printSchema()
    time_df.show(5, truncate=False)
    time_table_output = output_data+"times.parquet"
    time_df.write.mode("overwrite").partitionBy("year","month").parquet(time_table_output)

    df_songdata = spark.read.json(song_data_local)

    df_logsong_join = df_logdata_filter.join(df_songdata,(df_logdata_filter.artist    == df_songdata.artist_name)&\
                                                         (df_logdata_filter.song    == df_songdata.title))
    df_logsong_join = df_logsong_join.withColumn("songplay_id", monotonically_increasing_id())

    df_logsong_join.createOrReplaceTempView("songplays")

    songplays_df = spark.sql("""
                            SELECT songplay_id
                                  ,timestamp         AS start_time
                                  ,userId
                                  ,level
                                  ,song_id
                                  ,artist_id
                                  ,sessionId
                                  ,artist_location
                                  ,userAgent
                           FROM   songplays
                           ORDER BY (userId
                                    ,sessionId)
                          """)
    songplays_df.printSchema()
    songplays_df.show(5, truncate=False)
    songplays_output = output_data+"songplays.parquet"
    songplays_df.write.mode("overwrite").parquet(songplays_output)
    return users_df , time_df, songplays_df

def main():
    '''
    Main function to create spark session and process song data and log data
    Arguments : None
    '''
    spark                  =  create_spark_session()
    songs, artists         =  process_song_data(spark, song_data_local, output_data)
    users, time, songplays =  process_log_data(spark, song_data_local, log_data_local, output_data)
    print("ETL processing is done")

if __name__ == "__main__":
    main()
