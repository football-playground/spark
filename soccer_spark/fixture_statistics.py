import sys, os
sys.path.append(os.path.abspath('/home/kjh/code/football/spark'))

from pyspark.sql import SparkSession
from lib.etc import *

def spark_fixture_statistics(df, spark:SparkSession):
    from pyspark.sql.functions import first
    
    df.createOrReplaceTempView("fixtures")
    df_stat = spark.sql("""
                        SELECT 
                            response_item.fixture.id AS fixture_id,
                            stats_item.team.id AS team_id,
                            stat_detail.type,
                            stat_detail.value
                        FROM 
                            fixtures
                        LATERAL VIEW EXPLODE(response) AS response_item
                        LATERAL VIEW EXPLODE(response_item.statistics) AS stats_item
                        LATERAL VIEW EXPLODE(stats_item.statistics) AS stat_detail
                        """)
    df_pivoted = df_stat.groupBy("fixture_id", "team_id").pivot("type").agg(first("value"))
    return df_pivoted