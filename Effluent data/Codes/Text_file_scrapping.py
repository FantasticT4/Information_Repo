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

def get_txt_files(directory_path):
    """recursively list path of all csv files in path directory """
    txt_files = []
    files_to_treat = dbutils.fs.ls(directory_path)
    while files_to_treat:
        path = files_to_treat.pop(0).path
        if path.endswith('/'):
            files_to_treat += dbutils.fs.ls(path)
        elif path.endswith('.txt'):
            txt_files.append(path)
    return txt_files

# COMMAND ----------

files_path =get_txt_files(path)
files_path

# COMMAND ----------

myls=[]
for path in files_path:
    df = spark.read.text(path)
    my_df=df.selectExpr("split(value, ' ') as\
    Text_Data_In_Rows_Using_Text").toPandas()
    site_no =int(my_df.iloc[3][0][4])
    name= ' '.join(my_df.iloc[16][0])
    latitude =my_df.iloc[19][0][2]
    longitude =my_df.iloc[20][0][2]
    ls =[site_no, name, latitude, longitude]
    myls.append(ls)
    
schema = StructType([ \
    StructField("mon_feature_id",IntegerType(),True), \
    StructField("site_name",StringType(),True), \
    StructField("latitude",StringType(),True), \
    StructField("longitude", StringType(), True), \
  ])
 
df = spark.createDataFrame(data=myls,schema =schema)
df.show()

# COMMAND ----------

df.toPandas().info()

# COMMAND ----------

#Create CSV File
my_df=df.toPandas()

my_df.to_csv('site_data.csv', header=True,index=False)


# COMMAND ----------

#move from driver to blob storage.
path="wasb://intern-water-data@waterintern.blob.core.windows.net/merged-data/site_data.csv"
dbutils.fs.mv('file:/databricks/driver/site_data.csv', path, True )

# COMMAND ----------


