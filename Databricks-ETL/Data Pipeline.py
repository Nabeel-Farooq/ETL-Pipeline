# Databricks notebook source
from pyspark.sql.functions import *
import urllib 
import os
import boto3
from datetime import date, datetime, timedelta
import datetime 

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "+05:00")

# COMMAND ----------

directory = "/dbfs/mnt/maq-mixpanel-data-source/2579907/"
save_dir = "/dbfs/mnt/maq-data-warehouse/Mixpanel"

# COMMAND ----------

db_name = "Mixpanel"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"use {db_name}")

# COMMAND ----------

def json_read(path,save_path,new_save_path,dir):
    df_json1 = spark.read.format("json").option("recursiveFileLookup", "True").load(path)
    listColumns=df_json1.columns
    for x in listColumns:
        if x == "time":
            df_json1=df_json1.withColumn("timestamp1",from_unixtime(col("time"),"yyyy-MM-dd HH:mm:ss"))
        if x == "mp_browser_version":    
            df_json1=df_json1.withColumn("mp_browser_version",col("mp_browser_version").cast('double'))
        if x == "coupon_discount":    
            df_json1=df_json1.withColumn("coupon_discount",col("coupon_discount").cast('double')) 
        if x == "sikkay_discount_amount":    
            df_json1=df_json1.withColumn("sikkay_discount_amount",col("sikkay_discount_amount").cast('double')) 
        if x == "length_of_video_in_mins":    
            df_json1=df_json1.withColumn("length_of_video_in_mins",col("length_of_video_in_mins").cast('double'))      
    #display(df_json1)
    #print(df_json1.count())
    df_json1.write.mode("append").orc(save_path)
    df_json1.write.format("delta").mode("append").option("overwriteSchema", "true").option("mergeSchema", "true").save(new_save_path)
    df_json1.write.format("delta").mode("append").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable(dir)

# COMMAND ----------

def json_read_overwrite(path,save_path,new_save_path,dir):
    df_json1 = spark.read.format("json").option("recursiveFileLookup", "True").load(path)
    #print(df_json1.count())
    listColumns=df_json1.columns
    for x in listColumns:
        if x == "time":
            df_json1=df_json1.withColumn("timestamp1",from_unixtime(col("time"),"yyyy-MM-dd HH:mm:ss"))
        if x == "mp_browser_version":    
            df_json1=df_json1.withColumn("mp_browser_version",col("mp_browser_version").cast('double'))
        if x == "coupon_discount":    
            df_json1=df_json1.withColumn("coupon_discount",col("coupon_discount").cast('double'))
        if x == "sikkay_discount_amount":    
            df_json1=df_json1.withColumn("sikkay_discount_amount",col("sikkay_discount_amount").cast('double')) 
        if x == "length_of_video_in_mins":    
            df_json1=df_json1.withColumn("length_of_video_in_mins",col("length_of_video_in_mins").cast('double'))    
    df_json1.write.mode("overwrite").orc(save_path)
    df_json1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("mergeSchema", "true").save(new_save_path)
    df_json1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable(dir)

# COMMAND ----------

def create_folder(directory_name):
    s3_client = boto3.client(service_name='s3', aws_access_key_id=access_key,
                                      aws_secret_access_key=secret_key)
    bucket_name = aws_bucket_name #it's name of your folders
    s3_client.put_object(Bucket=bucket_name, Key=("Mixpanel/"+directory_name+'/'))

# COMMAND ----------

def display_parquet(save_path):
    df = spark.read.parquet(save_path)
    print(df.count())
    display(df)

# COMMAND ----------

list_dir =["mp_identity_mappings_data","mp_people_data","doubt_payment_successful","download_click","ds_conversion_page","downloads_screen","try_now_ds_conversion"]

# COMMAND ----------

print(list_dir)

# COMMAND ----------

index = 0 
for dir in os.listdir(directory):
    index +=1 
    if index > 109:
        print(dir)
    

# COMMAND ----------

#pp_payment_successful
def mixpanelpipeline(year,month,day):
    index = 0 
    for dir in os.listdir(directory): 
        path = "/mnt/maq-mixpanel-data-source/2579907/%s/%s/%s/%s/"%(dir,year,month,day)
        new_path = "/mnt/maq-mixpanel-data-source/2579907/%s"%dir
        check_path = "/dbfs/mnt/maq-mixpanel-data-source/2579907/%s/%s/%s/%s/"%(dir,year,month,day)
        save_path = "/mnt/maq-data-warehouse/Mixpanel/%s/"%dir
        new_save_path = "/mnt/maq-data-warehouse/Mixpanel-Delta/%s/"%dir
        folder_name = dir
        index +=1
        if index > 0:
            if os.path.exists(check_path):
                if dir not in list_dir:
                    print(path,index)
                    json_read(path,save_path,new_save_path,dir)
               
        

# COMMAND ----------

current_time = datetime.datetime.now() - timedelta(days=1)
day = str(current_time.day).zfill(2) 
month = str(current_time.month).zfill(2)
year = current_time.year

# COMMAND ----------


mixpanelpipeline(year,month,day)

# COMMAND ----------

#%sql
#select * from mixpanel.pp_open_paper_pdf order by timestamp1 desc

# COMMAND ----------

for dir in os.listdir(directory):
    if dir in list_dir:
        #print(dir)
        path = "/mnt/maq-mixpanel-data-source/2579907/%s/"%(dir)
        check_path = "/dbfs/mnt/maq-mixpanel-data-source/2579907/%s/"%(dir)
        save_path = "/mnt/maq-data-warehouse/Mixpanel/%s/"%dir
        new_save_path = "/mnt/maq-data-warehouse/Mixpanel-Delta/%s/"%dir
        folder_name = dir
        if os.path.exists(check_path):
            print(dir)
            json_read_overwrite(path,save_path,new_save_path,dir)
            #display_parquet(save_path)
        else:
            continue

# COMMAND ----------


