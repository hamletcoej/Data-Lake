import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This fuction sets up the spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads song_data from input S3 bucket and processes it by creating the songs_info and artist_info 
    then loading these back to output S3 bucket    
    """   
    
    # get filepath to song data file
    #song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json') # Used for testing process works succesfully
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year','duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+'song_info')

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_info')


    
def process_log_data(spark, input_data, output_data):
    """
    This function loads log_data from input S3 bucket and processes and creates user and time folders in the output bucket. 
    The song_info dataset (processed in 'process_song_data' function) is also pulled and joined with log data to create time 'songplay_info'
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table      
    df.createOrReplaceTempView("usr")
    users_table = spark.sql("""
                            SELECT DISTINCT userId as user_id, 
                            firstName as first_name,
                            lastName as last_name,
                            gender,
                            level,
                            ts
                            FROM usr
                            WHERE userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users') 
    
    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df = df.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))
    
    # extract columns to create time table
    time_table = df.select('start_time')
    time_table = time_table.withColumn('hour', F.hour('start_time'))
    time_table = time_table.withColumn('day', F.dayofmonth('start_time'))
    time_table = time_table.withColumn('week', F.weekofyear('start_time'))
    time_table = time_table.withColumn('month', F.month('start_time'))
    time_table = time_table.withColumn('year', F.year('start_time'))
    time_table = time_table.withColumn('weekday', F.dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])#overwrite helps

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'song_info')
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')
    
    # extract columns and join song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT
                                events.songplay_id,
                                events.start_time,
                                events.userId as user_id,
                                events.level,
                                songs.song_id,
                                songs.artist_id,
                                events.sessionId as session_id,
                                events.location,
                                events.userAgent as user_agent,
                                year(events.start_time) as year,
                                month(events.start_time) as month
                                FROM events
                                JOIN songs ON events.song = songs.title
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_info')


def main():
    """
    This is the main function that executes the other functions defined above.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://1stawbucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
