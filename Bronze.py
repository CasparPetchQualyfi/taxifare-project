# Databricks notebook source
spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net", "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")
spark_df = spark.read.format('csv').option('header', True).load("abfss://landing-cp@datacohortworkspacelabs.dfs.core.windows.net") 

# COMMAND ----------

spark_df.display()

# COMMAND ----------

from pyspark.sql.functions import lit
from  pyspark.sql.functions import input_file_name

spark_df_2 = spark_df.withColumn("FileNameLong", input_file_name())
spark_df_2.display()

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import datetime


udf1 = udf(lambda x:x[-27:],StringType())
spark_df_3 = spark_df_2.withColumn('FileName', udf1('FileNameLong')).withColumn("CreatedOn", lit(datetime.datetime.now()))
spark_df_3.display()

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net",
    "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")
spark_df_3.write.format("delta").mode("overwrite").partitionBy("vendor_name").save("abfss://bronze-cp@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata")
