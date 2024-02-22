import sys, os
sys.path.append(os.path.abspath('/home/kjh/code/football/spark'))
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col
from lib.etc import *

def readStream(topic:str, partition:int, spark:SparkSession):
    
    kafka_bootstrap_servers = get_config_values([("kafka", "host")])[0]
    assign = f"""{{"{topic}":[{partition}]}}"""
    
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
      .option("assign", assign) \
      .load()
         
    return df

def writeStream(df, spark:SparkSession, mode:str, save_location:str, checkpoint:str, func):
    transform = load_transform(save_location, mode, func)
    query = df \
    .writeStream \
    .foreachBatch(transform) \
    .option("checkpointLocation", checkpoint) \
    .start()
    return query

    
# def stream(topic:str, partition:int, save_location:str, checkpoint:str, spark:SparkSession):
#     def transform(batchdf, batchid):
#         df_value = batchdf.selectExpr("CAST(value AS STRING) as json")
#         rdd = spark.sparkContext.parallelize([df_value.select("json")])
#         df_fin = spark.read.json(rdd, multiLine=True)
#         df_fin.show()
#         save_dataframe(df_fin, save_location)
    
#     kafka_bootstrap_servers = 'localhost:9092'
#     assign = f"""{{"{topic}":[{partition}]}}"""
    
#     df = spark \
#         .readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#         .option("assign", assign) \
#         .load()
         
#     query = df \
#         .writeStream \
#         .foreachBatch(transform) \
#         .option("checkpointLocation", checkpoint) \
#         .start()

#     query.awaitTermination()




    

# def writeStream(buffer:list, batch:list, buffer_size:int, df, spark:SparkSession, save_location:str, checkpoint:str, func):
#     query = df \
#     .writeStream \
#     .foreachBatch(lambda batchDF, batchID: set_buffer(buffer, batch, buffer_size, batchDF, batchID, save_location, spark, func)) \
#     .option("checkpointLocation", checkpoint) \
#     .start()

#     query.awaitTermination()