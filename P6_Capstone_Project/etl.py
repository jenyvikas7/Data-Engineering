import pandas as pd
import re
from pyspark.sql import SparkSession
import os
import glob
import configparser
from datetime import datetime, timedelta, date
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col, monotonically_increasing_id,count,when,date_format
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,first,lower
from pyspark.sql import *
from pyspark.sql.types import IntegerType,LongType,StructType,StructField,StringType
import datetime
spark = SparkSession.builder.\
config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
.getOrCreate()


# Read US Cities Demo dataset file
df_city_temp=spark.read.csv("us-cities-demographics.csv", sep=';', header=True)

# Selecting columns of our interest from df_city
city_columns=['City','State','Male Population','Female Population','Total Population','Race','Count']
# Creating a new data frame from df_city_temp dataframe
df_city = df_city_temp.select(city_columns)
# We will clean this dataframe further and try to remove the duplicates with loosing any information
df_city_race = df_city.select('City','State','Race','Count').groupBy('City','State').pivot('Race').agg(first('Count'))

# Dropping unnecessary columns from df_city_temp
df_city_temp = df_city_temp.drop("Number of Veterans").drop("Race").drop("Count").drop('Median Age').drop("Foreign-born").dropDuplicates()

# We will join df_city_temp dataframe with df_city_race to denormalize the data
city = df_city_temp.join(df_city_race,["City","State"])

# We will join df_city_temp dataframe with df_city_race to denormalize the data
city = city.select("City","State",col("Male Population").alias("Male_Population"),
                     col("Female Population").alias("Female_Population"),col("Total Population").alias("Total_Population"),
                     col("State Code").alias("State_Code"), 
                     col("American Indian and Alaska Native").alias("Native_P"),
                     col("Asian").alias("Asian_P"),
                     col("Black or African-American").alias("African_P"),
                     col("Hispanic or Latino").alias("Latino_P"),
                     col("White").alias("White_P"))

# We will join df_city_temp dataframe with df_city_race to denormalize the data
city = city.withColumn("Male_Pop",city.Male_Population.cast(IntegerType()))\
                      .withColumn("Female_Pop",city.Female_Population.cast(IntegerType()))\
                      .withColumn("Total_Pop",city.Total_Population.cast(IntegerType()))\
                      .withColumn("Native_Pop",city.Native_P.cast(IntegerType()))\
                      .withColumn("Asian_Pop",city.Asian_P.cast(IntegerType()))\
                      .withColumn("African_Pop",city.African_P.cast(IntegerType()))\
                      .withColumn("Latino_Pop",city.Latino_P.cast(IntegerType()))\
                      .withColumn("White_Pop",city.White_P.cast(IntegerType()))
drop_cols=['Male_Population','Female_Population','Total Population','Native_P','Asian_P','African_P',
          'Latino_P','White_P']
city = city.drop(*drop_cols)

#Writing this transformed dataframe to Parquet files partitioned by State
city.write.partitionBy('State').parquet(os.path.join('city_data','cities.parquet'),'overwrite')

#Reading airport data
df_airport=spark.read.csv("airport-codes_csv.csv",header=True)

# Selecting columns of our interest from df_airport
airport_columns=['type','name','iso_country','iso_region','municipality']
# Creating a new data frame from airport dataframe
df_airport = df_airport.select(airport_columns)

#Writing this transformed dataframe to Parquet files partitioned by type
df_airport.write.partitionBy('type').parquet(os.path.join('airport_data','airport.parquet'),'overwrite')


# Reading i94 sample data
df_i94=spark.read.parquet("sas_data")

#Casting the values appropriately
df_i94=df_i94.select(col("i94res").cast(IntegerType()),col("i94port"),
                           col("arrdate").cast(IntegerType()),
                           col("i94mode").cast(IntegerType()),col("depdate").cast(IntegerType()),
                           col("i94bir").cast(IntegerType()),col("i94visa").cast(IntegerType()), 
                           col("count").cast(IntegerType()),
                           col("gender"),col("admnum").cast(LongType()))

#Admission number is supposed to be unique
df_i94 = df_i94.dropDuplicates(['admnum'])

# Read port_list text file
i94_port_df = pd.read_csv('./data/port_list.txt',sep='=',names=['id','port'])

# Remove whitespaces and single quotes
i94_port_df['id']=i94_port_df['id'].str.strip().str.replace("'",'')

# Create two columns from i94port string: port_city and port_addr
# also remove whitespaces and single quotes
i94_port_df['portcity'], i94_port_df['portstate']=i94_port_df['port'].str.strip().str.replace("'",'').str.strip().str.split(',',1).str

# Remove more whitespace from port_addr
i94_port_df['portstate']=i94_port_df['portstate'].str.strip()

# Drop port column and keep the two new columns: port_city and port_addr
i94_port_df.drop(columns =['port'], inplace = True)

# Convert pandas dataframe to list (objects which had single quotes removed automatically become string again with single quotes)
i94_port_data=i94_port_df.values.tolist()
i94_port_df.head()
i94port_schema = StructType([
    StructField('id', StringType(), True),
    StructField('portcity', StringType(), True),
    StructField('portstate', StringType(), True)
])
i94port=spark.createDataFrame(i94_port_data, i94port_schema)
i94port.write.mode('overwrite').parquet('data/i94port.parquet')

#Creating list for mode of entry
i94_mode=[[1,'Air'],[2,'Sea'],[3,'Land'],[9,'Not Reported']]

#Creating list for mode of entry
i94_mode_df=spark.createDataFrame(i94_mode)
i94_mode_df.write.mode('overwrite').parquet('data/i94_mode.parquet')

# Reading text file for countries
i94res_df = pd.read_csv('data/country_list.txt',sep='=',names=['id','country'])
# Remove whitespaces and single quotes
i94res_df['country']=i94res_df['country'].str.replace("'",'').str.strip()
# Convert pandas dataframe to list
i94res_data=i94res_df.values.tolist()

# Converting list to spark dataframe
i94res_schema = StructType([
    StructField('id', StringType(), True),
    StructField('country', StringType(), True)
])
i94res=spark.createDataFrame(i94res_data, i94res_schema)

# Creating parquet file
i94res.write.mode('overwrite').parquet('data/i94res.parquet')

#Creating dataframe for visa type
i94visa_data = [[1, 'Business'], [2, 'Pleasure'], [3, 'Student']]
i94visa=spark.createDataFrame(i94visa_data)
i94visa.write.mode('overwrite').parquet('./data/i94visa.parquet')

#Joining i94 dataframe with i94port dataframe
df_i94=df_i94.join(i94port,df_i94.i94port==i94port.id,how='left')

# Dropping off id column
df_i94 = df_i94.drop('id')

#Joining i94 dataframe with city dataframe
i94_city= df_i94.join(city,(lower(df_i94.portcity)==lower(city.City)) & (lower(df_i94.portstate)==lower(city.State)),how='left')

# Dropping off City and State columuns from the joined dataframe
i94_city=i94_city.drop("City","State")

#Converting SAS date to datetime
get_date = udf(lambda x: (datetime.datetime(1960, 1, 1).date() + datetime.timedelta(x)).isoformat() if x else None)
i94_city = i94_city.withColumn("arrival_date", get_date(i94_city.arrdate))

# Creating dataframe for date dimensions
i94date=i94_city.select(col('arrdate').alias('arrival_sasdate'),
                                   col('arrival_date').alias('arrivaldate'),
                                   date_format('arrival_date','M').alias('arrival_month'),
                                   date_format('arrival_date','E').alias('arrival_dayofweek'), 
                                   date_format('arrival_date', 'y').alias('arrival_year'), 
                                   date_format('arrival_date', 'd').alias('arrival_day'),
                                  date_format('arrival_date','w').alias('arrival_weekofyear')).dropDuplicates()

# Joining date dataframe with i94 dataframe
i94_full = i94_city.join(i94date,i94_city.arrdate==i94date.arrival_sasdate,how='left')

# Writing i94 data to parquet files partitioned by year and month
i94_full.write.partitionBy('arrival_year','arrival_month').parquet(os.path.join('i94_data','i94.parquet'),'overwrite')

