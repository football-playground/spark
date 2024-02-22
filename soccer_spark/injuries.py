from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

def spark_injuries(df, spark:SparkSession):
    injuries = df
    injuries.createOrReplaceTempView("injury")
    injury = spark.sql("""
                select 
                    explode(response) AS response_item,
                    response_item.player.id as player_id,
                    response_item.player.type as injury_type,
                    response_item.player.reason as injury_reason,
                    response_item.team.id as team_id,
                    response_item.fixture.id as fixture_id,
                    response_item.league.id as league_id 
                from injury
               """)
    return injury