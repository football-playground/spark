from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

spark = SparkSession.builder \
    .appName("player_sideline") \
    .getOrCreate()
def spark_player_sideline(Path,save_location):
    player_sideline = spark.read.json(Path,multiLine=True)
    player_sideline.createOrReplaceTempView("player_sideline")
    player_side_line = spark.sql("""WITH 
                            df_player_sideline as (SELECT 
                            parameters.player AS player_id,
                            EXPLODE(response) AS expanded_sideline,
                            MONOTONICALLY_INCREASING_ID() AS index_player
                            FROM player_sideline)
                            SELECT
    player_id,
    expanded_sideline.type AS sideline_type,
    expanded_sideline.start AS sideline_start,
    expanded_sideline.end AS sideline_end
FROM
    df_player_sideline;
""")
    return player_side_line.write.parquet(save_location)