from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

spark = SparkSession.builder \
    .appName("coach_sideline") \
    .getOrCreate()
def spark_coach_sideline(Path,save_location):
    coach_sideline = spark.read.json(Path,multiLine=True)
    coach_sideline.createOrReplaceTempView("coach_sideline")
    coach_side_line = spark.sql("""WITH 
                            df_coach_sideline as (SELECT 
                            parameters.coach AS player_id,
                            EXPLODE(response) AS expanded_sideline,
                            MONOTONICALLY_INCREASING_ID() AS index_coach
                            FROM coach_sideline)
                            SELECT
    coach_id,
    expanded_sideline.type AS sideline_type,
    expanded_sideline.start AS sideline_start,
    expanded_sideline.end AS sideline_end
FROM
    df_coach_sideline;
""")
    return coach_side_line.write.parquet(save_location)