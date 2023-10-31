# Databricks notebook source
# PySpark - Apply custom schema to a DataFrame by changing names 

# Import the libraries SparkSession, StructType, 
# StructField, StringType, IntegerType 
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# Create a spark session using getOrCreate() function 
spark_session = SparkSession.builder.getOrCreate() 


spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

# Applying custom schema to data frame 
#df = spark.read.csv("wasbs://bronze-cp@datacohortworkspacelabs.blob.core.windows.net/yellow_tripdata", header=True, schema=schema)
df = spark_session.read.format( 
	"delta").option( 
	"header", True).load("abfss://silver-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata") 

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import round, hash
df = df.withColumn("Start_Lon_Round", round(df["Start_Lon"], 4))
df = df.withColumn("Start_Lat_Round", round(df["Start_Lat"], 4))
df = df.withColumn("End_Lon_Round", round(df["End_Lon"], 4))
df = df.withColumn("End_Lat_Round", round(df["End_Lat"], 4))

# COMMAND ----------

from pyspark.sql.functions import rand


df = df.withColumn("Customer_Review", round(rand()*5)).withColumn("Waiting_Time", rand()*20)


df = df.withColumn("vendor_id", hash(df["vendor_name"])).withColumn("pickup_id", hash(df["Start_Lon_Round"], df["Start_Lat_Round"])).withColumn("dropoff_id", hash(df["End_Lon_Round"], df["End_Lat_Round"])).withColumn("transaction_id", hash(df["Fare_Amt"], df["surcharge"], df["mta_tax"], df["Tip_Amt"], df["Total_Amt"]))

# COMMAND ----------

df.display()

# COMMAND ----------

dimension_table_vendor = df.select("vendor_name", "vendor_id")
dimension_table_pickup_location = df.select("Start_Lon_Round", "Start_Lat_Round", "pickup_id")
dimension_table_dropoff_location = df.select("End_Lon_Round", "End_Lat_Round", "dropoff_id")
dimension_table_transaction_details = df.select("Payment_Type", "Fare_Amt", "surcharge", "mta_tax", "Tip_Amt", "Tolls_Amt", "Total_Amt", "transaction_id")

fact_table = df.select("Trip_Pickup_DateTime", "Trip_Dropoff_DateTime", "Passenger_Count", "Trip_Distance", "Waiting_Time", "Customer_Review", "vendor_id", "pickup_id", "dropoff_id", "transaction_id")



# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net",
    "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")
fact_table.write.format("delta").mode("overwrite").save("abfss://gold-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_fact")

# COMMAND ----------

dimension_table_vendor = dimension_table_vendor.distinct()
dimension_table_pickup_location = dimension_table_pickup_location.distinct()
dimension_table_dropoff_location = dimension_table_dropoff_location.distinct()
dimension_table_transaction_details = dimension_table_transaction_details.distinct()

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net",
    "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")
dimension_table_vendor.write.format("delta").mode("overwrite").save("abfss://gold-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_dim_vendor")
dimension_table_pickup_location.write.format("delta").mode("overwrite").save("abfss://gold-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_dim_pickup")
dimension_table_dropoff_location.write.format("delta").mode("overwrite").save("abfss://gold-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_dim_dropoff")
dimension_table_transaction_details.write.format("delta").mode("overwrite").save("abfss://gold-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_dim_transaction")
