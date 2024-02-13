from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

spark = SparkSession.builder \
    .appName("fixture_statistics") \
    .getOrCreate()
def spark_fixture_statistics(Path,save_location):
    fixture_statistics = spark.read.json(Path, multiLine=True)
    df_fixture_team = fixture_statistics.select(
        col("parameters.fixture").alias("fixture_id"),
        explode("response.team").alias("expanded_team"),
        monotonically_increasing_id().alias("index_fixture")
    )

    # Create a DataFrame for statistics information
    df_fixture_statistics = fixture_statistics.select(
        explode("response.statistics").alias("expanded_statistics"),
        monotonically_increasing_id().alias("index_statistics_type")
    )

    # Join the DataFrames on the generated index

    df_fixture_statistics_type_exploded = df_fixture_statistics.select(
        explode("expanded_statistics.type").alias("statistics_type"),
        monotonically_increasing_id().alias("index_statistics_type")
    )
    df_fixture_statistics_value_exploded = df_fixture_statistics.select(
        explode("expanded_statistics.value").alias("statistics_value"),
        monotonically_increasing_id().alias("index_statistics_value")
    )
    column_names = [row['statistics_type'] for row in df_fixture_statistics_type_exploded.collect()]
    values = [row['statistics_value'] for row in df_fixture_statistics_value_exploded.collect()]

    df_joined = df_fixture_team.join(
        df_fixture_statistics_type_exploded,
        df_fixture_team["index_fixture"] == df_fixture_statistics_type_exploded["index_statistics_type"],
        "inner"
    )
    column_value_mapping = dict(zip(column_names, values))

    # Dynamically add columns to df_joined with values from df_fixture_statistics_value_exploded
    for stat_type, value in column_value_mapping.items():
        df_joined = df_joined.withColumn(stat_type, lit(value).cast("string"))

    df_result = df_joined.select(
        "fixture_id",
        col("expanded_team.id").alias("team_id"),
        "Shots on Goal","Shots off Goal","Total Shots","Blocked Shots","Shots insidebox","Shots outsidebox","Fouls","Corner Kicks","Offsides","Ball Possession","Yellow Cards","Red Cards","Goalkeeper Saves","Total passes","Passes accurate","Passes %"
    )
    return df_result.write.parquet(save_location)