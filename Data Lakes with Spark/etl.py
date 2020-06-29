import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# Get AWS access keys
config = configparser.ConfigParser()
config.read('dl.cfg')

# Access keys are set prior to creating Spark session
os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS",'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Create spark session.
    
    Returns
    -------
    spark 
        spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data by reading from S3, transforming tables in Spark,
    and writing to S3.
    
    Parameters
    ----------
    spark
        spark session
    input_data : str
        path to read input data
    output_data : str
        path to read output data
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create view
    df.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT 
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_latitude,
            artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """Process log data by reading from S3, transforming tables in Spark,
    and writing to S3.
    
    Parameters
    ----------
    spark
        spark session
    input_data : str
        path to read input data
    output_data : str
        path to read output data
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # create view
    df.createOrReplaceTempView("staging_events")
    
    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT 
            userId AS user_id, 
            firstName AS first_name,
            lastName AS last_name,
            gender, 
            level
        FROM staging_events
        WHERE user_id IS NOT NULL
    """)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"users/")

    # create start_time column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000.0)
    df = df.withColumn("start_time", get_timestamp(df.ts))
    df.createOrReplaceTempView("staging_events")
    
    # create udfs for time cuts
    spark.udf.register("get_hour", lambda x: datetime.fromtimestamp(x).hour)
    spark.udf.register("get_day", lambda x: datetime.fromtimestamp(x).day)
    spark.udf.register("get_week", lambda x: datetime.fromtimestamp(x).isocalendar()[1])
    spark.udf.register("get_month", lambda x: datetime.fromtimestamp(x).month)
    spark.udf.register("get_year", lambda x: datetime.fromtimestamp(x).year)
    spark.udf.register("get_weekday", lambda x: datetime.fromtimestamp(x).weekday())
    
    
    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT 
            start_time, 
            get_hour(start_time) AS hour,
            get_day(start_time) AS day,
            get_week(start_time) AS week,
            get_month(start_time) AS month,
            get_year(start_time) AS year,
            get_weekday(start_time) AS weekday
        FROM staging_events
        WHERE start_time IS NOT NULL
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time_table/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table/")
    song_df.createOrReplaceTempView("staging_songs")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT DISTINCT 
            events.start_time,
            events.userId AS user_id,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionId AS session_id,
            events.location,
            events.userAgent AS user_agent
        FROM staging_songs songs
        JOIN staging_events events
            ON events.artist = songs.artist_name
            AND events.song = songs.title
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays_table/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sc-udacity-project/"

    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()


if __name__ == "__main__":
    main()
