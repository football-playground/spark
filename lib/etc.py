import os
from configparser import ConfigParser
from pyspark.sql import SparkSession, DataFrame

def get_config_values(sets:list):
    # create config
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),"../config/config.ini")
    config = ConfigParser()
    config.read(config_path)
    
    # read values
    returned = [config.get(category, key) for (category, key) in sets]
    
    return returned

def load_transform(save_location:str, mode, func):
    def transform(batchdf, batchid):
        try:
            spark = SparkSession.getActiveSession()
            df_string = batchdf \
                .selectExpr("CAST(value AS STRING) as json") \
                .first().asDict()
            rdd = spark.sparkContext.parallelize([df_string["json"]])
            df_json = spark.read.json(rdd, multiLine=True)
            df_json.show()
            df_fin = func(df_json, spark)
            df_fin.show()
            df_fin \
                .write \
                .mode(mode) \
                .format("parquet") \
                .option("path", save_location) \
                .save()
        except Exception as e:
            print(f">>>>>> Oops, error appeared.. \n{e}")
    return transform


# def export_all(batchDF, spark:SparkSession):
#     from pyspark.sql.functions import col
#     from pyspark.sql.types import StringType, ArrayType

#     first_row_dict = batchDF \
#         .withColumn("key", col("key").cast(StringType())) \
#         .withColumn("value", col("value").cast(StringType())) \
#         .select(*batchDF.columns).first().asDict() # 전체 컬럼 데이터를 선정하여 dict로 변환

#     print(first_row_dict) # test

#     return first_row_dict

# def create_dataframe(buffer:list, spark:SparkSession):
#     import json
#     from pyspark.sql import Row
    
#     rdd = spark.sparkContext.parallelize([item['value'] for item in buffer])
#     df = spark.read.json(rdd, multiLine=True)

#     df.show() # test
       
#     return df

# def print_dataframe(df, spark:SparkSession):
#     df \
#         .write \
#         .format("console") \
#         .save()

# def save_dataframe(df, save_location:str):
#     df \
#         .write \
#         .mode("append") \
#         .format("parquet") \
#         .option("path", save_location) \
#         .save()
        


# def set_buffer(buffer:list, batch:list, buffer_size:int, batchDF, batchId, save_location:str, spark:SparkSession, func):
#     try:
#         data = export_all(batchDF=batchDF, spark=spark) # buffer 데이터(dict) 생성
#         buffer_fix.append(data)
#         batch_fix.append(batchId)
        
#         if len(buffer) >= buffer_size:
#             df_buffer = create_dataframe(buffer=buffer, spark=spark) # dataframe 생성
#             df_dict = func(df_buffer, spark)
#             if len(df_dict) == 1:
#                 (key, df) = next(iter(df_dict.items()))
#                 save_dataframe(df, save_location, spark)
#             else:
#                 for key, df in df_dict.items():
#                     dir = f'{save_location}/{df_dict[key]}'
#                     save_dataframe(df, dir, spark) # batch 작업 수행
#             print(">>>>>> Let's check out batches " + str(batch)) # batch 확인 / need to fix
#             buffer, batch = [], [] # buffer 및 batch 초기화

    
#     except Exception as E: # 초기 blank batchDF에 대한 예외처리 필요
#         print(f">>>>>> Oops, error appeared.. \n{E}")
        
# def set_buffer_bak(batchDF, batchId, save_location:str, spark:SparkSession, func):
#     try:
#         global buffer, batch
#         data = export_all(batchDF=batchDF, spark=spark) # buffer 데이터(dict) 생성
#         buffer.append(data)
#         batch.append(batchId)
        
#         if len(buffer) >= buffer_size:
#             df_buffer = create_dataframe(buffer=buffer, spark=spark) # dataframe 생성
#             df_transform = func(df_buffer, spark)
#             save_dataframe(df_transform, save_location, spark) # batch 작업 수행
#             print(">>>>>> Let's check out batches " + str(batch)) # batch 확인 / need to fix
#             buffer, batch = [], [] # buffer 및 batch 초기화

    
#     except Exception as E: # 초기 blank batchDF에 대한 예외처리 필요
#         print(f">>>>>> Oops, error appeared.. \n{E}")