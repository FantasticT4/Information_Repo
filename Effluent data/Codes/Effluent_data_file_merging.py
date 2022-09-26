# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import urllib
import matplotlib.pyplot as plt
import glob
import os

# COMMAND ----------

storage_account_name = "waterintern"
storage_account_access_key = "xGaT4LUanysVUm8O2SqmZdJ4UfLZdGOgbs7NsSDDPY9SoG7PGB5ZmcC6CfNbWCKSblg4Tx45DN18+AStF6156A=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

path= "wasb://intern-water-data@waterintern.blob.core.windows.net/Extracted-zipped/"
dbutils.fs.ls(path)


# COMMAND ----------

# MAGIC %md
# MAGIC schema = StructType() \
# MAGIC     .add("mon_feature_id",IntegerType(), True)\
# MAGIC     .add("date_time", StringType(), True)\
# MAGIC     .add("sample_begin_depth",FloatType(), True)\
# MAGIC     .add("institution_abbr",StringType(), True)\
# MAGIC     .add("preservative_abbr",StringType(), True)\
# MAGIC     .add("Ca_Diss_Water",FloatType(), True)\
# MAGIC     .add("Cl_Diss_Water",FloatType(), True)\
# MAGIC     .add("DMS_Tot_Water",FloatType(), True)\
# MAGIC     .add("EC_Phys_Water",FloatType(), True)\
# MAGIC     .add("F_Diss_Water",FloatType(), True)\
# MAGIC     .add("K_Diss_Water",FloatType(), True)\
# MAGIC     .add("KJEL_N_Tot_Water",FloatType(), True)\
# MAGIC     .add("Mg_Diss_Water",FloatType(), True)\
# MAGIC     .add("Na_Diss_Water",FloatType(), True)\
# MAGIC     .add("NH4_N_Diss_Water",FloatType(), True)\
# MAGIC     .add("NO3_NO2_N_Diss_Water",FloatType(), True)\
# MAGIC     .add("P_Tot_Water",FloatType(), True)\
# MAGIC     .add("pH_Diss_Water",FloatType(), True)\
# MAGIC     .add("PO4_P_Diss_Water",FloatType(), True)\
# MAGIC     .add("Si_Diss_Water",FloatType(), True)\
# MAGIC     .add("SO4_Diss_Water",FloatType(), True)\
# MAGIC     .add("TAL_Diss_Water",FloatType(), True)\
# MAGIC     .add("Station",StringType(), True)\
# MAGIC     .add("Qat",StringType(), True)

# COMMAND ----------

schema = StructType() \
    .add("mon_feature_id",IntegerType(), False)\
    .add("date_time", StringType(), True)\
    .add("sample_begin_depth",StringType(), True)\
    .add("institution_abbr",StringType(), True)\
    .add("preservative_abbr",StringType(), True)\
    .add("Ca_Diss_Water",StringType(), True)\
    .add("Cl_Diss_Water",StringType(), True)\
    .add("DMS_Tot_Water",StringType(), True)\
    .add("EC_Phys_Water",StringType(), True)\
    .add("F_Diss_Water",StringType(), True)\
    .add("K_Diss_Water",StringType(), True)\
    .add("KJEL_N_Tot_Water",StringType(), True)\
    .add("Mg_Diss_Water",StringType(), True)\
    .add("Na_Diss_Water",StringType(), True)\
    .add("NH4_N_Diss_Water",StringType(), True)\
    .add("NO3_NO2_N_Diss_Water",StringType(), True)\
    .add("P_Tot_Water",StringType(), True)\
    .add("pH_Diss_Water",StringType(), True)\
    .add("PO4_P_Diss_Water",StringType(), True)\
    .add("Si_Diss_Water",StringType(), True)\
    .add("SO4_Diss_Water",StringType(), True)\
    .add("TAL_Diss_Water",StringType(), True)\
    .add("Station",StringType(), True)\
    .add("Qat",StringType(), True)

# COMMAND ----------

def get_csv_files(directory_path):
    """recursively list path of all csv files in path directory """
    csv_files = []
    files_to_treat = dbutils.fs.ls(directory_path)
    while files_to_treat:
        path = files_to_treat.pop(0).path
        if path.endswith('/'):
            files_to_treat += dbutils.fs.ls(path)
        elif path.endswith('.csv'):
            csv_files.append(path)
    return csv_files

# COMMAND ----------

files_path =get_csv_files(path)
list_dfs=[spark.read \
     .format('csv') \
     .option('header',True) \
     .option("sep", ",") \
     .option("recursiveFileLookup","true")\
     .load(x) for x in files_path]

# COMMAND ----------

from functools import reduce
def unite_dfs(df1, df2):
    return df1.unionByName(df2, allowMissingColumns=True)
df = reduce(unite_dfs, list_dfs)

# COMMAND ----------

df.toPandas().info()

# COMMAND ----------

df_final = df.withColumn("date_time", F.to_timestamp(F.col("date_time"))) 
df_final =df_final.na.drop(subset=["mon_feature_id"])
df_final =df_final[df_final["mon_feature_id"]!=1]

# COMMAND ----------

df_final.toPandas().info()

# COMMAND ----------

#Create parquet file
path="wasb://intern-water-data@waterintern.blob.core.windows.net/merged-data/effluent"
my_df = df_final.coalesce(1)
my_df.write.option("header",True).option("index",False).parquet(path)

# COMMAND ----------

#Create CSV File
my_df=my_df.toPandas()

my_df.to_csv('effluent.csv', header=True,index=False)


# COMMAND ----------

#move from driver to blob storage.
path="wasb://intern-water-data@waterintern.blob.core.windows.net/merged-data/effluent.csv"
dbutils.fs.mv('file:/databricks/driver/effluent.csv', path, True )

# COMMAND ----------


