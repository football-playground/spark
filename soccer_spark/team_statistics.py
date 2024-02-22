from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,count,explode,col,monotonically_increasing_id,lit

def spark_team_statistics(df, spark:SparkSession):
    df_team_statistics = df

    df_team_league = df_team_statistics.select(
    col("league.id").alias("league_id"),
    col("league.season").alias("season"),
    monotonically_increasing_id().alias("index_league")
)
    df_team= df_team_statistics.select( col("team.id").alias("team_id"),
        monotonically_increasing_id().alias("index_team"))


    df_league_team = df_team_league.join(df_team,df_team_league['index_league']==df_team['index_team'],"inner")
    df_form= df_team_statistics.select( col("form").alias("team_form"),
                                    monotonically_increasing_id().alias("index_form"))
    df_league_form = df_league_team.join(df_form,df_league_team["index_league"]==df_form['index_form'],"inner")
    df_fixtures = df_team_statistics.select(
        col("fixtures.played.home").alias("home_game"),
        col("fixtures.played.away").alias("away_game"),
        col("fixtures.played.total").alias("total_game"),
        col("fixtures.wins.home").alias("home_wins"),
        col("fixtures.wins.away").alias("away_wins"),
        col("fixtures.wins.total").alias("total_wins"),
        col("fixtures.draws.home").alias("home_draws"),
        col("fixtures.draws.away").alias("away_draws"),
        col("fixtures.draws.total").alias("total_draws"),
        col("fixtures.loses.home").alias("home_loses"),
        col("fixtures.loses.away").alias("away_loses"),
        col("fixtures.loses.total").alias("total_loses"),
        monotonically_increasing_id().alias("index_fixtures")
    )

    df_league_fixture =df_league_form.join(df_fixtures,df_league_form['index_league']==df_fixtures['index_fixtures'],"inner")

    df_goals=df_team_statistics.select(
        col("goals.for.total.home").alias("home_goals"),
        col("goals.for.total.away").alias("away_goals"),
        col("goals.for.total.total").alias("total_goals"),
        col("goals.for.average.home").alias("home_avg_goals"),
        col("goals.for.average.away").alias("away_avg_goals"),
        col("goals.for.average.total").alias("total_avg_goals"),
        col("goals.for.minute.0-15.total").alias("0-15_total_goals"),
        col("goals.for.minute.0-15.percentage").alias("0-15_goals_percentage"),
        col("goals.for.minute.16-30.total").alias("16-30_total_goals"),
        col("goals.for.minute.16-30.percentage").alias("16-30_goals_percentage"),
        col("goals.for.minute.31-45.total").alias("31-45_total_goals"),
        col("goals.for.minute.31-45.percentage").alias("31-45_goals_percentage"),
        col("goals.for.minute.46-60.total").alias("46-60_total_goals"),
        col("goals.for.minute.46-60.percentage").alias("46-60_goals_percentage"),
        col("goals.for.minute.61-75.total").alias("61-75_total_goals"),
        col("goals.for.minute.61-75.percentage").alias("61-75_goals_percentage"),
        col("goals.for.minute.76-90.total").alias("76-90_total_goals"),
        col("goals.for.minute.76-90.percentage").alias("76-90_goals_percentage"),
        col("goals.for.minute.91-105.total").alias("91-105_total_goals"),
        col("goals.for.minute.91-105.percentage").alias("91-105_goals_percentage"),
        col("goals.for.minute.106-120.total").alias("106-120_total_goals"),
        col("goals.for.minute.106-120.percentage").alias("106-120_goals_percentage"),
        col("goals.against.minute.0-15.percentage").alias("0-15_against_percentage"),
        col("goals.against.minute.16-30.total").alias("16-30_total_against_goals"),
        col("goals.against.minute.16-30.percentage").alias("16-30_against_percentage"),
        col("goals.against.minute.31-45.total").alias("31-45_total_againsT_goals"),
        col("goals.against.minute.31-45.percentage").alias("31-45_against_percentage"),
        col("goals.against.minute.46-60.total").alias("46-60_total_against_goals"),
        col("goals.against.minute.46-60.percentage").alias("46-60_against_percentage"),
        col("goals.against.minute.61-75.total").alias("61-75_total_against_goals"),
        col("goals.against.minute.61-75.percentage").alias("61-75_against_percentage"),
        col("goals.against.minute.76-90.total").alias("76-90_total_against_goals"),
        col("goals.against.minute.76-90.percentage").alias("76-90_against_percentage"),
        col("goals.against.minute.91-105.total").alias("91-105_total_against_goals"),
        col("goals.against.minute.91-105.percentage").alias("91-105_against_percentage"),
        col("goals.against.minute.106-120.total").alias("106-120_total_against_goals"),
        col("goals.against.minute.106-120.percentage").alias("106-120_against_percentage"),
        monotonically_increasing_id().alias("index_goals")
    )

    df_fixture_goals=df_league_fixture.join(df_goals,df_league_fixture['index_fixtures']==df_goals['index_goals'],'inner')

    df_biggest=df_team_statistics.select(
        col("biggest.streak.wins").alias("biggest_streak_wins"),
        col("biggest.streak.draws").alias("biggest_streak_draws"),
        col("biggest.streak.loses").alias("biggest_streak_loses"),
        col("biggest.wins.home").alias("biggest_home_wins"),
        col("biggest.wins.away").alias("biggest_away_wins"),
        col("biggest.loses.home").alias("biggest_home_loses"),
        col("biggest.loses.away").alias("biggest_away_loses"),
        col("biggest.goals.for.home").alias("biggest_home_goals"),
        col("biggest.goals.for.away").alias("biggest_away_goals"),
        col("biggest.goals.against.home").alias("biggest_home_against"),
        col("biggest.goals.against.away").alias("biggest_away_against"),
        monotonically_increasing_id().alias("index_biggest")
        
    )
    df_fixture_biggest = df_fixture_goals.join(df_biggest,df_fixture_goals['index_fixtures']==df_biggest['index_biggest'],'inner')

    df_clean_sheet=df_team_statistics.select(
        col("clean_sheet.home").alias("home_clean_sheet"),
        col("clean_sheet.away").alias("away_clean_sheet"),
        col("clean_sheet.total").alias("total_clean_sheet"),
        monotonically_increasing_id().alias("index_clean_sheet")
        
    )
    df_fixture_clean = df_fixture_biggest.join(df_clean_sheet,df_fixture_biggest['index_fixtures']==df_clean_sheet['index_clean_sheet'],'inner')
    df_failed_to_score=df_team_statistics.select(
        col("failed_to_score.home").alias("home_failed_to_score"),
        col("failed_to_score.away").alias("away_failed_to_score"),
        col("failed_to_score.total").alias("total_failed_to_score"),
        monotonically_increasing_id().alias("index_failed_to_score")
        
    )
    df_fixture_fail_score = df_fixture_clean.join(df_failed_to_score,df_fixture_clean['index_fixtures']==df_failed_to_score['index_failed_to_score'],'inner')

    df_penalty=df_team_statistics.select(
        col("penalty.scored.total").alias("total_penalty_scored"),
        col("penalty.scored.percentage").alias("total_penalty_scored_percentage"),
        col("penalty.missed.total").alias("total_penalty_missed"),
        col("penalty.missed.percentage").alias("total_penalty_missed_percentage"),
        col("penalty.total").alias("total_penalty"),
        monotonically_increasing_id().alias("index_penalty")
    )
    df_fixture_penalty = df_fixture_fail_score.join(df_penalty,df_fixture_fail_score['index_fixtures']==df_penalty['index_penalty'],'inner')

    # df_lineups=df_team_statistics.select(
    #     explode("lineups").alias("expanded_lineups"),
    #     monotonically_increasing_id().alias("index_lineups")
    #  )



    df_cards=df_team_statistics.select(
        col("cards.yellow.0-15.total").alias("0-15_yellow"),
        col("cards.yellow.0-15.percentage").alias("0-15_yellow_percentage"),
        col("cards.yellow.16-30.total").alias("16-30_yellow"),
        col("cards.yellow.16-30.percentage").alias("16-30_yellow_percentage"),
        col("cards.yellow.31-45.total").alias("31-45_yellow"),
        col("cards.yellow.31-45.percentage").alias("31-45_yellow_percentage"),
        col("cards.yellow.46-60.total").alias("46-60_yellow"),
        col("cards.yellow.46-60.percentage").alias("46-60_yellow_percentage"),
        col("cards.yellow.61-75.total").alias("61-75_yellow"),
        col("cards.yellow.61-75.percentage").alias("61-75_yellow_percentage"),
        col("cards.yellow.76-90.total").alias("76-90_yellow"),
        col("cards.yellow.76-90.percentage").alias("76-90_yellow_percentage"),
        col("cards.yellow.91-105.total").alias("91-105_yellow"),
        col("cards.yellow.91-105.percentage").alias("91-105_yellow_percentage"),
        col("cards.yellow.106-120.total").alias("106-120_yellows"),
        col("cards.yellow.106-120.percentage").alias("106-120_yellow_percentage"),
        col("cards.red.0-15.total").alias("0-15_red"),
        col("cards.red.0-15.percentage").alias("0-15_red_percentage"),
        col("cards.red.16-30.total").alias("16-30_red"),
        col("cards.red.16-30.percentage").alias("16-30_red_percentage"),
        col("cards.red.31-45.total").alias("31-45_red"),
        col("cards.red.31-45.percentage").alias("31-45_red_percentage"),
        col("cards.red.46-60.total").alias("46-60_red"),
        col("cards.red.46-60.percentage").alias("46-60_red_percentage"),
        col("cards.red.61-75.total").alias("61-75_red"),
        col("cards.red.61-75.percentage").alias("61-75_red_percentage"),
        col("cards.red.76-90.total").alias("76-90_red"),
        col("cards.red.76-90.percentage").alias("76-90_red_percentage"),
        col("cards.red.91-105.total").alias("91-105_red"),
        col("cards.red.91-105.percentage").alias("91-105_red_percentage"),
        col("cards.red.106-120.total").alias("106-120_red"),
        col("cards.red.106-120.percentage").alias("106-120_red_percentage"),
        monotonically_increasing_id().alias("index_cards")
        
    )
    df_fixture_card = df_fixture_penalty.join(df_cards,df_fixture_penalty['index_fixtures']==df_cards['index_cards'],'inner')

    # # "expanded_element"에서 필요한 정보 추출
    df_result = df_fixture_card.select(
    "index_league", "league_id","season","team_id","team_form","home_game","away_game","total_game","home_wins","away_wins","total_wins","home_draws","away_draws","total_draws","home_loses","away_loses","total_loses","home_goals","away_goals","total_goals","home_avg_goals","away_avg_goals","total_avg_goals","0-15_total_goals","0-15_goals_percentage","16-30_total_goals","16-30_goals_percentage","31-45_total_goals","31-45_goals_percentage","46-60_total_goals","46-60_goals_percentage","61-75_total_goals","61-75_goals_percentage","76-90_total_goals","76-90_goals_percentage","91-105_total_goals","91-105_goals_percentage","106-120_total_goals","106-120_goals_percentage","0-15_against_percentage","16-30_total_against_goals","16-30_against_percentage","31-45_total_against_goals","31-45_against_percentage","46-60_total_against_goals","46-60_against_percentage","61-75_total_against_goals","61-75_against_percentage","76-90_total_against_goals","76-90_against_percentage","91-105_total_against_goals","91-105_against_percentage","106-120_total_against_goals","106-120_against_percentage","biggest_streak_wins","biggest_streak_draws","biggest_streak_loses","biggest_home_wins","biggest_away_wins","biggest_home_loses","biggest_away_loses","biggest_home_goals","biggest_away_goals","biggest_home_against","biggest_away_against","home_clean_sheet","away_clean_sheet","total_clean_sheet","home_failed_to_score","away_failed_to_score","total_failed_to_score","total_penalty_scored","total_penalty_scored_percentage","total_penalty_missed","total_penalty_missed_percentage","total_penalty","0-15_yellow","0-15_yellow_percentage","16-30_yellow","16-30_yellow_percentage","31-45_yellow","31-45_yellow_percentage","46-60_yellow","46-60_yellow_percentage","61-75_yellow","61-75_yellow_percentage","76-90_yellow","76-90_yellow_percentage","91-105_yellow","91-105_yellow_percentage","106-120_yellows","106-120_yellow_percentage","0-15_red","0-15_red_percentage","16-30_red","16-30_red_percentage","31-45_red","31-45_red_percentage","46-60_red","46-60_red_percentage","61-75_red","61-75_red_percentage","76-90_red","76-90_red_percentage","91-105_red","91-105_red_percentage","106-120_red","106-120_red_percentage"

    )
    return df_result