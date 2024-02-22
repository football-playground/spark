import sys, os
from itertools import chain

from lib.sparkstreaming import *
sys.path.append(os.path.abspath('/home/kjh/code/football/spark'))

from pyspark.sql import SparkSession
from soccer_spark.fixture import spark_fixture
from soccer_spark.fixture_event import spark_fixture_event
from soccer_spark.fixture_statistics import spark_fixture_statistics
from soccer_spark.injuries import spark_injuries
from soccer_spark.team_statistics import spark_team_statistics
from soccer_spark.player_statistics import spark_player_statistics
from soccer_spark.coach_sideline import spark_coach_sideline
from soccer_spark.player_sidelined import spark_player_sideline


    
spark = SparkSession.builder \
    .appName("Spark Streaming - Kafka") \
    .getOrCreate()

def setup_and_start_stream(topic:str, partition:int, endpoint:str, func, spark:SparkSession):
    if endpoint == "fixtures" or func == None:
        mode = 'append'
        df = readStream(topic, partition, spark)
        dir_info = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}/info"
        dir_even = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}/events"
        dir_stat = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}/statistics"
        checkpoint_info = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}/info"
        checkpoint_even = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}/events"
        checkpoint_stat = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}/statistics"
        query_info = writeStream(df,  spark, mode, dir_info, checkpoint_info, spark_fixture)
        query_even = writeStream(df,  spark, mode, dir_even, checkpoint_even, spark_fixture_event)
        query_stat = writeStream(df,  spark, mode, dir_stat, checkpoint_stat, spark_fixture_statistics)
        return [query_info, query_even, query_stat]
    elif endpoint == "injuries":
        mode = 'append'
        df = readStream(topic, partition, spark)
        save_location = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}"
        checkpoint = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}"
        query = writeStream(df,  spark, mode, save_location, checkpoint, func)
        return [query]
    else:
        mode = 'overwrite'
        df = readStream(topic, partition, spark)
        save_location = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}"
        checkpoint = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}"
        query = writeStream(df,  spark, mode, save_location, checkpoint, func)
        return [query]

stream_configs = [
    {'topic': 'england', 'partition': 0, 'endpoint': 'fixtures', 'func': None},
    {'topic': 'england', 'partition': 1, 'endpoint': 'injuries', 'func': spark_injuries},
    {'topic': 'england', 'partition': 2, 'endpoint': 'teamstat', 'func': spark_team_statistics},
    {'topic': 'england', 'partition': 3, 'endpoint': 'playerstat', 'func': spark_player_statistics},
    {'topic': 'spain', 'partition': 0, 'endpoint': 'fixtures', 'func': None},
    {'topic': 'spain', 'partition': 1, 'endpoint': 'injuries', 'func': spark_injuries},
    {'topic': 'spain', 'partition': 2, 'endpoint': 'teamstat', 'func': spark_team_statistics},
    {'topic': 'spain', 'partition': 3, 'endpoint': 'playerstat', 'func': spark_player_statistics},
    {'topic': 'italy', 'partition': 0, 'endpoint': 'fixtures', 'func': None},
    {'topic': 'italy', 'partition': 1, 'endpoint': 'injuries', 'func': spark_injuries},
    {'topic': 'italy', 'partition': 2, 'endpoint': 'teamstat', 'func': spark_team_statistics},
    {'topic': 'italy', 'partition': 3, 'endpoint': 'playerstat', 'func': spark_player_statistics},
    {'topic': 'germany', 'partition': 0, 'endpoint': 'fixtures', 'func': None},
    {'topic': 'germany', 'partition': 1, 'endpoint': 'injuries', 'func': spark_injuries},
    {'topic': 'germany', 'partition': 2, 'endpoint': 'teamstat', 'func': spark_team_statistics},
    {'topic': 'germany', 'partition': 3, 'endpoint': 'playerstat', 'func': spark_player_statistics},
    {'topic': 'france', 'partition': 0, 'endpoint': 'fixtures', 'func': None},
    {'topic': 'france', 'partition': 1, 'endpoint': 'injuries', 'func': spark_injuries},
    {'topic': 'france', 'partition': 2, 'endpoint': 'teamstat', 'func': spark_team_statistics},
    {'topic': 'france', 'partition': 3, 'endpoint': 'playerstat', 'func': spark_player_statistics},
    {'topic': 'sidelined', 'partition': 0, 'endpoint': 'coach', 'func': spark_coach_sideline},
    {'topic': 'sidelined', 'partition': 1, 'endpoint': 'player', 'func': spark_player_sideline},
]

queries = list(chain.from_iterable(setup_and_start_stream(**config, spark=spark) for config in stream_configs))

for query in queries:
    query.awaitTermination()





# def process(topic:str, partition:int, endpoint:str, func, spark:SparkSession):
#     df = readStream(topic, partition, spark)
#     save_location = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}"
#     checkpoint = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}"
#     query = writeStream(df,  spark, save_location, checkpoint, func)
#     return query

# def process_fixtures(topic:str, partition:int, endpoint:str, spark:SparkSession):
#     df = readStream(topic, partition, spark)
#     dir_info = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}/info"
#     dir_events = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}/events"
#     dir_statistics = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}/statistics"
#     checkpoint_info = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}/info"
#     checkpoint_events = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}/events"
#     checkpoint_statistics = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}/statistics"
#     query_info = writeStream(df,  spark, dir_info, checkpoint_info, spark_fixture)
#     query_events = writeStream(df,  spark, dir_events, checkpoint_events, spark_fixture_event)
#     query_statistics = writeStream(df,  spark, dir_statistics, checkpoint_statistics, spark_fixture_statistics)
#     return query_info, query_events, query_statistics
# def process(buffer:list, batch:list, topic:str, partition:int, endpoint:str, func, spark:SparkSession):
#     buffer_size = 5
#     df = readStream(topic, partition, spark)
#     save_location = f"{os.path.dirname(os.path.abspath(__file__))}/datas/{topic}/{endpoint}"
#     checkpoint = f"{os.path.dirname(os.path.abspath(__file__))}/checkpoint/{topic}/{endpoint}"
#     writeStream(buffer, batch, buffer_size, df,  spark, save_location, checkpoint, func)

# query_eng_fixtures_info, query_eng_fixtures_events, query_eng_fixtures_statistics = process_fixtures('england', 0, 'fixtures', spark)
# query_eng_injuries = process('england', 1, 'injuries', spark_injuries, spark)
# query_eng_teamstat = process('england', 2, 'teamstat', spark_team_statistics, spark)
# query_eng_playstat = process('england', 1, 'playerstat', spark_player_statistics, spark)
# query_spa_fixtures_info, query_spa_fixtures_events, query_spa_fixtures_statistics = process_fixtures('england', 0, 'fixtures', spark)
# query_spa_injuries = process('england', 1, 'injuries', spark_injuries, spark)
# query_spa_teamstat = process('england', 2, 'teamstat', spark_team_statistics, spark)
# query_spa_playstat = process('england', 1, 'playerstat', spark_player_statistics, spark)
# query_ita_fixtures_info, query_ita_fixtures_events, query_ita_fixtures_statistics = process_fixtures('england', 0, 'fixtures', spark)
# query_ita_injuries = process('england', 1, 'injuries', spark_injuries, spark)
# query_ita_teamstat = process('england', 2, 'teamstat', spark_team_statistics, spark)
# query_ita_playstat = process('england', 1, 'playerstat', spark_player_statistics, spark)
# query_ger_fixtures_info, query_ger_fixtures_events, query_ger_fixtures_statistics = process_fixtures('england', 0, 'fixtures', spark)
# query_ger_injuries = process('england', 1, 'injuries', spark_injuries, spark)
# query_ger_teamstat = process('england', 2, 'teamstat', spark_team_statistics, spark)
# query_ger_playstat = process('england', 1, 'playerstat', spark_player_statistics, spark)
# query_fra_fixtures_info, query_fra_fixtures_events, query_fra_fixtures_statistics = process_fixtures('england', 0, 'fixtures', spark)
# query_fra_injuries = process('england', 1, 'injuries', spark_injuries, spark)
# query_fra_teamstat = process('england', 2, 'teamstat', spark_team_statistics, spark)
# query_fra_playstat = process('england', 1, 'playerstat', spark_player_statistics, spark)
# query_coachsidelined = process('sidelined', 0, 'coachsidelined', spark_coach_sideline, spark)
# query_playersidelined = process('sidelined', 1, 'playersidelined', spark_player_sideline, spark)

# query_eng_fixtures_info.awaitTermination()
# query_eng_fixtures_events.awaitTermination()
# query_eng_fixtures_statistics.awaitTermination()
# query_eng_injuries.awaitTermination()
# query_eng_teamstat.awaitTermination()
# query_eng_playstat.awaitTermination()
# query_spa_fixtures_info.awaitTermination()
# query_spa_fixtures_events.awaitTermination()
# query_spa_fixtures_statistics.awaitTermination()
# query_spa_injuries.awaitTermination()
# query_spa_teamstat.awaitTermination()
# query_spa_playstat.awaitTermination()
# query_ita_fixtures_info.awaitTermination()
# query_ita_fixtures_events.awaitTermination()
# query_ita_fixtures_statistics.awaitTermination()
# query_ita_injuries.awaitTermination()
# query_ita_teamstat.awaitTermination()
# query_ita_playstat.awaitTermination()
# query_ger_fixtures_info.awaitTermination()
# query_ger_fixtures_events.awaitTermination()
# query_ger_fixtures_statistics.awaitTermination()
# query_ger_injuries.awaitTermination()
# query_ger_teamstat.awaitTermination()
# query_ger_playstat.awaitTermination()
# query_fra_fixtures_info.awaitTermination()
# query_fra_fixtures_events.awaitTermination()
# query_fra_fixtures_statistics.awaitTermination()
# query_fra_injuries.awaitTermination()
# query_fra_teamstat.awaitTermination()
# query_fra_playstat.awaitTermination()
# query_coachsidelined.awaitTermination()
# query_playersidelined.awaitTermination()