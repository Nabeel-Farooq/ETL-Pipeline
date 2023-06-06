# Databricks notebook source
import os
import json
from glob import glob
from json import JSONDecodeError
import re
from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.types import DateType, TimestampType,BooleanType,IntegerType
from pyspark import SparkContext
spark = SparkSession.builder.appName("AdjustPipeline").getOrCreate()
sc = spark.sparkContext


class PipelineDate:
    def __init__(self,month,year,day):
        self.month = month
        self.year = year
        self.day = day



class AdjustPipeline:

    def __init__(self,config):
       #self.topic_data_path =  config.get("topic_data_path")
        self.topic_path =  config.get("topic_path") 
        self.pipeline_date = AdjustPipeline.get_pipeline_date()

    def get_topic_data_path(topic_name,topic_path,pipeline_date):
        #return "{topic_path}{topic_name}/year={year}/month={month}/day={day}/*.json".format(topic_path=topic_path,topic_name=topic_name,year=pipeline_date.year,month=pipeline_date.month,day=pipeline_date.day)
        return "{topic_path}{topic_name}/*.csv".format(topic_path=topic_path,topic_name=topic_name)

    def get_pipeline_date():
        current_time = datetime.now() - timedelta(days=26)
        day = str(current_time.day).zfill(2) 
        month = str(current_time.month).zfill(2)
        year = current_time.year
        return PipelineDate(month,year,day)
 


    def read_data(self,path):
        df_csv = spark.read.format("csv").option("recursiveFileLookup", "True").option("header",True).load(path)
        return df_csv
      
    def init_db(self,string):
        db_name = string
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        spark.sql(f"use {db_name}")
        

    def rename_columns(self,df_csv):
        df_csv = df_csv.withColumnRenamed("[user_id]","user_id").withColumnRenamed("{installed_at}","installed_at").withColumnRenamed("{event_name}","event_name").withColumnRenamed("{adid}","adid").withColumnRenamed("{campaign_name}","campaign_name").withColumnRenamed("{adgroup_name}","adgroup_name").withColumnRenamed("{creative_name}","creative_name").withColumnRenamed("{is_organic}","is_organic").withColumnRenamed("{installed_at_hour}","installed_at_hour").withColumnRenamed("{network_name}","network_name").withColumnRenamed("{activity_kind}","activity_kind").withColumnRenamed("{created_at}","created_at")
        df_csv = df_csv.withColumn('created_at', from_unixtime('created_at').cast(DateType()))
        df_csv = df_csv.withColumn('installed_at', from_unixtime('installed_at').cast(DateType()))
        df_csv = df_csv.withColumn('installed_at_hour', from_unixtime('installed_at_hour').cast(TimestampType()))
        tran_tab = str.maketrans({x:None for x in list('{()}')})
        df_csv = df_csv.toDF(*(re.sub(r'[\.\s]+', '_', c).translate(tran_tab) for c in df_csv.columns))
        return df_csv
    
    def schema_data_type(self,df_csv):
        df_csv = df_csv.withColumn("is_organic",col("is_organic").cast(BooleanType()))
        return df_csv
      
    def write_data_delta(self,df_csv,path,string,table_name):
        df_csv.write.format("delta").mode(string).option("overwriteSchema", "true").save(path)
        df_csv.write.format("delta").mode(string).option("overwriteSchema", "true").saveAsTable(table_name)

    def write_data(self,df_csv,path,string):
        df_csv = df_csv.write.mode(string).orc(path)
    
        
    def write_adjust_event(self,df_csv,string,events):
        df_csv.createOrReplaceTempView("parquet_file")
        df_csv = spark.sql("select * from parquet_file where activity_kind = '%s'"%events)
        save_path = "/mnt/maq-data-warehouse/Dev/Adjust/Adjust-%s/"%events
        save_path_delta = "/mnt/maq-data-warehouse/Dev/Adjust-Delta/Adjust-%s/"%events
        AdjustPipeline.write_data_delta(df_csv,save_path_delta,"Overwrite",events)
        df_csv.write.mode(string).orc(save_path)

# COMMAND ----------

adjust_pipeline = AdjustPipeline({"topic_path": "/dbfs/mnt/maq-adjust-data-source/", "topic_data_path": "/dbfs/mnt/maq-adjust-data-source/*.csv"})
adjust_events = ["impression","event","install","session","uninstall","reinstall","click","reattribution_reinstall","install_update","sk_install","sk_event","sk_qualifier","reattribution_update","click","att_update","reattribution"]

def adjust():          
    save_path = "/mnt/maq-data-warehouse/Dev/Adjust/Adjust"
    save_path_delta = "/mnt/maq-data-warehouse/Dev/Adjust-Delta/Adjust"
    adjust_pipeline.init_db("adjustDev")
    df_csv = adjust_pipeline.read_data("/mnt/maq-adjust-data-source/")
    df_csv = adjust_pipeline.rename_columns(df_csv)
    df_csv = adjust_pipeline.schema_data_type(df_csv)
    adjust_pipeline.write_data(df_csv,save_path,"overwrite")
    adjust_pipeline.write_data_delta(df_csv,save_path_delta,"overwrite","Adjust")
    df_csv = adjust_pipeline.read_data(save_path)
    for x in adjust_events:
        print(x)
        adjust_pipeline.write_adjust_event(df_csv,"overwrite",x)
        
    

def main():
    adjust()
    

# COMMAND ----------

if __name__ == "__main__":
    main()

# COMMAND ----------


