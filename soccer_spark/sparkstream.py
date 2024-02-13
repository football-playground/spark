from pyspark.sql import SparkSession
from lib.kafkaconsumer import *
from fixture import spark_fixture
from fixture_event import spark_fixture_event
from fixture_statistics import spark_fixture_statistics
# from fixture_lineups import spark_fixture_lineups
from injuries import spark_injuries
# from team_statistics import spark_team_statistics
from player_statistics import spark_player_statistics
from coach_sideline import spark_coach_sideline
# from player_sideline import spark_player_sideline


spark = SparkSession.builder \
    .appName("Spark Streaming - Kafka") \
    .getOrCreate()