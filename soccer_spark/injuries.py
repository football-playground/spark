from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

spark = SparkSession.builder \
    .appName("injuries") \
    .getOrCreate()
def spark_injuries(Path,save_location):
    injuries = spark.read.json(Path,multiLine=True)
    injuries.createOrReplaceTempView("injury")
    injury = spark.sql("""select response.player.id[0] as player_id,
                   response.player.type[0] as injury_type,
                    response.player.reason[0] as injury_reason,
                   response.team.id[0] as team_id,
                   response.fixture.id[0] as fixture_id,
                   response.league.id[0] as league_id from injury
""")
    return injury.write.parquet(save_location)