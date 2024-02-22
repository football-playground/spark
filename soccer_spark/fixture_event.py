import sys, os
sys.path.append(os.path.abspath('/home/kjh/code/football/spark'))

from pyspark.sql import SparkSession
from lib.etc import *

def spark_fixture_event(df,spark:SparkSession):
    from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit
    
    fixture_events = df
    df_exploded_fixtures = fixture_events.select(
        explode("response").alias("response_item")
    )
    df_fixture_events= df_exploded_fixtures.select(
        col("response_item.fixture.id").alias("fixture_id"),
        explode("response_item.events").alias("expanded_event")
    )
    df_result = df_fixture_events.select(
        "fixture_id",
        col("expanded_event.time.elapsed").alias("time_elapsed"),
        col("expanded_event.time.extra").alias("time_extra"),
        col("expanded_event.team.id").alias("team_id"),
        col("expanded_event.player.id").alias("player_id"),
        col("expanded_event.assist.id").alias("assist_id"),
        col("expanded_event.type").alias("event_type"),
        col("expanded_event.detail").alias("event_detail"),
        col("expanded_event.comments").alias("event_comments")
    )
    return df_result