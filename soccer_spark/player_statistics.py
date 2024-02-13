from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

spark = SparkSession.builder \
    .appName("player_statistics") \
    .getOrCreate()
def spark_statistics(Path,save_location):
    player_statistics = spark.read.json(Path,multiLine=True)
    player_statistics.createOrReplaceTempView("player_statistics")
    player_stat = spark.sql("""WITH
    expanded_players AS (
        SELECT
            explode(response.player) as expanded_player,
            monotonically_increasing_id() as index_player 
        FROM
            player_statistics
    ),
    expanded_statistics AS (
        SELECT
            explode(response.statistics) as expanded_statistics,
            monotonically_increasing_id() AS index_statistics
        FROM
            player_statistics
    ),
        df_player_stat AS (
        SELECT
            expanded_players.*,
            expanded_statistics.*
        FROM
            expanded_players
            JOIN expanded_statistics ON expanded_players.index_player = expanded_statistics.index_statistics
    )
        SELECT
            expanded_player.id as player_id,
            expanded_statistics.team.id[0] as team_id ,
            expanded_statistics.league.id[0] AS league_id,
            expanded_statistics.league.season[0] AS league_season,
            expanded_statistics.games.appearences[0] AS game_appearances,
            expanded_statistics.games.lineups[0] AS game_lineups,
            expanded_statistics.games.minutes[0] AS game_minutes,
            expanded_statistics.games.number[0] AS game_number,
            expanded_statistics.games.position[0] AS game_position,
            expanded_statistics.games.rating[0] AS game_rating,
            expanded_statistics.games.captain[0] AS game_captain,
            expanded_statistics.substitutes.in[0] AS sub_in,
            expanded_statistics.substitutes.out[0] AS sub_out,
            expanded_statistics.substitutes.bench[0] AS sub_bench,
            expanded_statistics.shots.total[0] AS total_shots,
            expanded_statistics.shots.on[0] AS shots_on,
            expanded_statistics.goals.total[0] AS total_goals,
            expanded_statistics.goals.conceded[0] AS goals_conceded,
            expanded_statistics.goals.assists[0] AS goals_assists,
            expanded_statistics.goals.saves[0] AS goals_saves,
            expanded_statistics.passes.total[0] AS passes_total,
            expanded_statistics.passes.key[0] AS passes_key,
            expanded_statistics.passes.accuracy[0] AS passes_accuracy,
            expanded_statistics.tackles.total[0] AS tackles_total,
            expanded_statistics.tackles.blocks[0] AS tackles_blocks,
            expanded_statistics.tackles.interceptions[0] AS tackles_interceptions,
            expanded_statistics.duels.total[0] AS duels_total,
            expanded_statistics.duels.won[0] AS duels_won,
            expanded_statistics.dribbles.attempts[0] AS dribbles_attempts,
            expanded_statistics.dribbles.success[0] AS dribbles_success,
            expanded_statistics.dribbles.past[0] AS dribbles_past,
            expanded_statistics.fouls.drawn[0] AS fouls_drawn,
            expanded_statistics.fouls.committed[0] AS fouls_committed,
            expanded_statistics.cards.yellow[0] AS cards_yellow,
            expanded_statistics.cards.yellowred[0] AS cards_yellowred,
            expanded_statistics.cards.red[0] AS cards_red,
            expanded_statistics.penalty.won[0] AS penalty_won,
            expanded_statistics.penalty.commited[0] AS penalty_commited,
            expanded_statistics.penalty.scored[0] AS penalty_scored,
            expanded_statistics.penalty.missed[0] AS penalty_missed,
            expanded_statistics.penalty.saved[0] AS penalty_saved from df_player_stat""")
    return player_stat.write.parquet(save_location)