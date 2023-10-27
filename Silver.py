# Databricks notebook source
# PySpark - Apply custom schema to a DataFrame by changing names 

# Import the libraries SparkSession, StructType, 
# StructField, StringType, IntegerType 
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# Create a spark session using getOrCreate() function 
spark_session = SparkSession.builder.getOrCreate() 

# Define the structure for the data frame 
schema = StructType([ 
	StructField('Pickup_timestamp', 
				TimestampType(), True), 
	StructField('Dropoff_timestamp', 
				TimestampType(), True), 
	StructField('Passenger_count', 
				IntegerType(), True), 
	StructField('Trip_distance_km',
				FloatType(), True), 
	StructField('Waiting_time_minutes', 
				FloatType(), True),
    StructField('Customer_review_score', 
				IntegerType(), True) 
]) 


spark.conf.set(
"fs.azure.account.key.datacohortworkspacelabs.blob.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

# Applying custom schema to data frame 
#df = spark.read.csv("wasbs://bronze-cp@datacohortworkspacelabs.blob.core.windows.net/yellow_tripdata", header=True, schema=schema)
df = spark_session.read.format( 
	"delta").option( 
	"header", True).load("abfss://bronze-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata") 

# Display the updated schema 
df.display()


# COMMAND ----------

df_filtered = df.filter(df['Total_Amt']>0).filter(df['Trip_Distance']>0).filter(df['FileName'].contains('2019') | df['FileName'].contains('2014')).drop("FileNameLong")
df_filtered.display()

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net",
    "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")
df_filtered.write.format("delta").mode("overwrite").partitionBy("vendor_name").save("abfss://silver-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata")
