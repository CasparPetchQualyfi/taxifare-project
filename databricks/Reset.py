# Databricks notebook source
"""
DELETE ALL FILES IN CONTAINER
"""


spark.conf.set(
    "fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net",
    "NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")
 
 
dbutils.fs.rm("abfss://landing-cp@datacohortworkspacelabs.dfs.core.windows.net/", True)
dbutils.fs.rm("abfss://bronze-cp@datacohortworkspacelabs.dfs.core.windows.net/", True)
dbutils.fs.rm("abfss://silver-cp@datacohortworkspacelabs.dfs.core.windows.net/", True)
dbutils.fs.rm("abfss://gold-cp@datacohortworkspacelabs.dfs.core.windows.net/", True)
