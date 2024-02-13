from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

spark = SparkSession.builder \
    .appName("fixture_events") \
    .getOrCreate()
def spark_fixture_event(Path,save_location):
    fixture_events = spark.read.json(Path, multiLine=True)
    df_fixture_events= fixture_events.select(
        col("parameters.fixture").alias("fixture_id"),
        explode("response").alias("expanded_event")
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
    return df_result.write.parquet(save_location)