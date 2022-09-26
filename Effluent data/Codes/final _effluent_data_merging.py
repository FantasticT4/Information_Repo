# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import urllib

# COMMAND ----------

storage_account_name = "waterintern"
storage_account_access_key = "xGaT4LUanysVUm8O2SqmZdJ4UfLZdGOgbs7NsSDDPY9SoG7PGB5ZmcC6CfNbWCKSblg4Tx45DN18+AStF6156A=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

path= "wasb://intern-water-data@waterintern.blob.core.windows.net/merged-data/"
dbutils.fs.ls(path)


# COMMAND ----------

effluent_df=spark.read \
     .format('csv') \
     .option('header',True) \
     .option("sep", ",") \
     .load('wasb://intern-water-data@waterintern.blob.core.windows.net/merged-data/effluent.csv')

# COMMAND ----------

effluent_df = effluent_df.withColumn("date_time", F.to_timestamp(F.col("date_time"))) \
                         .withColumn("mon_feature_id",F.col("mon_feature_id").cast(IntegerType()))
effluent_df.toPandas().info()

# COMMAND ----------

site_df=spark.read \
     .format('csv') \
     .option('header',True) \
     .option("sep", ",") \
     .load('wasb://intern-water-data@waterintern.blob.core.windows.net/merged-data/site_data.csv')

# COMMAND ----------

site_df = site_df.withColumn("mon_feature_id",F.col("mon_feature_id").cast(IntegerType()))

# COMMAND ----------

site_df.toPandas().info()

# COMMAND ----------

effluent_df=effluent_df.coalesce(1)
df=effluent_df.join(site_df,on="mon_feature_id", how="right")

# COMMAND ----------

df.toPandas().info()

# COMMAND ----------

df_final=df.distinct()
df_final.toPandas().info()

# COMMAND ----------

#Create CSV File
my_df=df_final.toPandas()

my_df.to_csv('final_effluent.csv', header=True,index=False)


# COMMAND ----------

#move from driver to blob storage.
path="wasb://intern-water-data@waterintern.blob.core.windows.net/merged-data/final_effluent.csv"
dbutils.fs.mv('file:/databricks/driver/final_effluent.csv', path, True )
