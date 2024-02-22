from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

def spark_player_sideline(df, spark:SparkSession):
    player_sideline = df
    player_sideline.createOrReplaceTempView("player_sideline")
    player_side_line = spark.sql("""
                            SELECT
                                player_id,
                                type AS sideline_type,
                                start AS sideline_start,
                                end AS sideline_end
                            FROM
                                player_sideline;
                            """)
    return player_side_line