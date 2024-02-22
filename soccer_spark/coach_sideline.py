from pyspark.sql import SparkSession

def spark_coach_sideline(df, spark:SparkSession):
    coach_sideline = df
    coach_sideline.createOrReplaceTempView("coach_sideline")
    coach_side_line = spark.sql("""
                            SELECT
                                coach_id,
                                type AS sideline_type,
                                start AS sideline_start,
                                end AS sideline_end
                            FROM
                                coach_sideline;
                            """)
    return coach_side_line