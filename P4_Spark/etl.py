import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

def create_spark_session():
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    print('Getting filepath to song data file')
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    print('Got filepath to song data file: {}'.format(song_data))
    print('Reading song data file')
    df = spark.read.json(song_data)
    df.show(2)
    print('Read song data file')
    print('Extracting columns to create songs table')
    songs_table = df.select(['song_id', 'title', 'artist_id','artist_name', 'year', 'duration'])
    songs_table.show(2)
    print('Successfully extracted columns to create songs table')
    print('Writing songs table to parquet files partitioned by year and artist')
    songs_table = songs_table.dropDuplicates(subset=['song_id'])
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs.parquet'),'overwrite')
    print('Successfully wrote songs table to parquet files partitioned by year and artist')
    print('Extracting columns to create artists table')
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table.show(2)
    print('Successfully extracted columns to create artists table')
    artists_table = artists_table.dropDuplicates(subset=['artist_id'])
    artists_table.write.parquet(os.path.join(output_data,'artists.parquet'),'overwrite')
    print('Successfully wrote artists table to parquet files')
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    print('Getting filepath to log data file')
    log_data = os.path.join(input_data,'log_data/*.json')
    print('Got filepath to log data file: {}'.format(log_data))

    # read log data file
    print('Reading log data file')
    df = spark.read.json(log_data)
    df.show(2)
    print('Successfully read log data file')
    
    # filter by actions for song plays
    print('Filtering by actions for song plays')
    df = df.where(df.page=='NextSong')
    df.show(2)
    print('Filtered by actions for song plays')

    # extract columns for users table    
    print('Extracting columns for users table')
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level','ts'])
    #users_table = users_table.orderBy('ts',ascending=False).dropDuplicates(subset=['userId'])
    users_table.show(2)
    print('Successfully extracted columns for users table')
    
    # write users table to parquet files
    print('Writing users table to parquet files')
    users_table.write.parquet(os.path.join(output_data,'users.parquet'),'overwrite')
    print('Successfully wrote users table to parquet files')

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    print('Creating datetime column from original timestamp column')
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0))
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x: x.day)
    get_month = udf(lambda x: x.month)
    get_year = udf(lambda x: x.year)
    df = df.withColumn('start_time',get_datetime(df.ts))
    df = df.withColumn('hour',get_hour(df.start_time))
    df = df.withColumn('day',get_day(df.start_time))
    df = df.withColumn('month',get_month(df.start_time))
    df = df.withColumn('year',get_year(df.start_time))
    df.show(2)
    print('Created columns for hour,day,month,year from original timestamp column')
    
    
    # extract columns to create time table
    print('Extracting columns to create time table')
    time_table = df.select(['start_time', 'hour', 'day', 'month', 'year'])
    time_table.show(2)
    print('Successfully extracted columns to create time table')
    
    # write time table to parquet files partitioned by year and month
    print('Writing time table to parquet files partitioned by year and month')
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'time.parquet'),'overwrite')
    print('Successfully wrote time table to parquet files partitioned by year and month')

    # read in song data to use for songplays table
    print('Reading in song data to use for songplays table')
    song_df = spark.read.parquet('results/songs.parquet')
    song_df.show(2)
    print('Successfully read in song data to use for songplays table')

    # extract columns from joined song and log datasets to create songplays table 
    print('Extracting columns from joined song and log datasets to create songplays table ')
    df = df.join(song_df, (song_df.title == df.song) & (song_df.artist_name == df.artist))
    df = df.withColumn('songplay_id',monotonically_increasing_id())
    songplays_table = df[['songplay_id','start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']]
    songplays_table.show(2)
    print('Extracted columns from joined song and log datasets to create songplays table ')

    # 
    print('Write songplays table to parquet files partitioned by year and month')
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print('Successfully wrote songplays table to parquet files partitioned by year and month')

def main():
    spark = create_spark_session()
    input_data = "data"
    output_data = "results"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

    