import configparser
import os
import sys
sys.path.append('/home/dharani/Downloads/Spark/spark-3.0.0-preview2-bin-hadoop2.7/python')
sys.path.append('/home/dharani/Downloads/Spark/spark-3.0.0-preview2-bin-hadoop2.7/python/lib/py4j-0.10.8.1-src')
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id



config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

song_data_local = '/home/dharani/Udacity/DataLake/data/song-data/A/A/A'
log_data_local  = '/home/dharani/Udacity/DataLake/data/log-data'
output_data     = '/home/dharani/Udacity/DataLake/data/output_data'

def create_spark_session():
    spark = SparkSession\
            .builder\
            .master("local")\
            .appName("Spark local")\
            .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
            .getOrCreate()
    return spark

def process_song_data(  spark
                       ,song_data_local
                       ,output_data):
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
                        SELECT   artist_id           AS  artist_id
                                ,artist_name         AS  name
                                ,artist_location     AS  location
                                ,artist_latitude     AS  latitude
                                ,artist_longitude    AS  longitude
                        FROM    artists
                        WHERE   artist_id IS NOT NULL
                        GROUP BY  artist_id
                                 ,artist_name
                                 ,artist_location
                                 ,artist_latitude
                                 ,artist_longitude
                        ORDER BY artist_id;
                        """)
    artists_df.printSchema()
    artists_df.show(5, truncate=False)
    artists_table_output = output_data+"artists.parquet"
    artists_df.write.mode("overwrite").parquet(artists_table_output)

    return songs_df, artists_df

def process_log_data(  spark ,song_data_local,log_data_local ,output_data):
    df_logdata = spark.read.json(log_data_local)
    df_logdata.printSchema()

    df_logdata_filter = df_logdata.filter(df_logdata.page=='NextSong')
    df_logdata_filter.createOrReplaceTempView("users")
    users_df = spark.sql("""
                    SELECT  userId     AS   user_id
                           ,firstName  AS   first_name
                           ,lastName   AS   last_name
                           ,gender     AS   gender
                           ,level      AS   level
                    FROM    users
                    WHERE  userId IS NOT NULL
                    GROUP BY user_id
                            ,user_first_name
                            ,user_last_name
                            ,gender
                            ,user_level
                    ORDER BY userId;
                    """)
    users_df.printSchema()
    users_df.show(5, truncate=False)
    users_table_output = output_data+"users.parquet"
    users_df.write.mode("overwrite").parquet(users_table_output)

    @udf(t.TimestampType())
    def get_timestamp(ts):
        return datetime.fromtimestamp(ts/1000.0)
    df_logdata_filter = df_logdata_filter.withColumn('timestamp', get_timestamp(df_logdata_filter.ts))

    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0).strftime('%Y-%m-%d %H:%M:%S')
    df_logdata_filter = df_logdata_filter.withColumn('datetime',get_datetime(df_logdata_filter.ts))

    df_logdata_filter.printSchema()

    df_logdata_filter.show(5)

    df_logdata_filter.createOrReplaceTempView("times")
    time_df = spark.sql("""
                    SELECT  DISTINCT start_time
                               ,date_part(hour,    start_time) AS hour
                               ,date_part(day ,    start_time) AS day
                               ,date_part(week,    start_time) AS week
                               ,date_part(month,   start_time) AS month
                               ,date_part(year,    start_time) AS year
                               ,date_part(weekday, start_time) AS weekday
                        FROM   times
                        WHERE    start_time IS NOT NULL
                        ORDER BY start_time;
                        """)
    time_df.printSchema()
    time_df.show(5, truncate=False)
    time_table_output = output_data+"times.parquet"
    time_df.write.mode("overwrite").partitionBy("year","month").parquet(time_table_output)

    df_songdata = spark.read.json(song_data_local)

    df_logsong_join = df_logdata_filter.join(df_songdata,(df_logdata_filter.artists==df_songdata.artist_name)&\
                                                         (df_logdata_filter.song == df_songdata.title))
    df_logsong_join = df_logsong_join.withColumn("songplay_id", monotonically_increasing_id())

    df_logsong_join.createOrReplaceTempView("songplays")

    songplays_df = spark.sql("""
                           SELECT songplay_id        AS songplay_id
                                  ,timestamp         AS start_time
                                  ,user_id           AS user_id
                                  ,user_level        AS level
                                  ,song_id           AS song_id
                                  ,artist_id         AS artist_id
                                  ,session_id        AS session_id
                                  ,artist_location   AS location
                                  ,user_agent        AS user_agent
                           FROM   songplays
                           ORDER BY (user_id
                                    ,session_id)
                         """)
    songplays_df.printSchema()
    songplays_df.show(5, truncate=False)
    songplays_output = output_data+"songplays.parquet"
    songplays_df.write.mode("overwrite").parquet(songplays_output)

    return users_df , time_df, songplays_df

def main():
    spark                  =  create_spark_session()
    songs, artists         =  process_song_data(spark, song_data_local, output_data)
    users, time, songplays =  process_log_data(spark,song_data_local , log_data_local, output_data)
    print("ETL processing is done")
if __name__ == "__main__":
    main()
