from pyspark.sql import SparkSession

def consumer(topic:str, partition:int, spark:SparkSession):
    kafka_bootstrap_servers = 'localhost:9092'
    assign = f"""{{"{topic}":[{partition}]}}"""
    
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
      .option("assign", assign) \
      .load()
      
    # JSON 문자열로 변환
    json_string  = json.dumps(playlist_info)

    # JSON 문자열을 RDD로 변환
    json_rdd = spark.sparkContext.parallelize([json_string])

    # RDD를 사용하여 DataFrame 생성 (multiline 옵션 사용)
    df_plinfo = spark.read.json(json_rdd, multiLine=True)
      
    return df