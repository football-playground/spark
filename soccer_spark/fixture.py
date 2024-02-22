import sys, os
sys.path.append(os.path.abspath('/home/kjh/code/football/spark'))

from pyspark.sql import SparkSession
from lib.etc import *

def spark_fixture(df, spark:SparkSession):
    df_fixtures = df
    df_fixtures.createOrReplaceTempView("df_fixtures")
    df_fixture_info=spark.sql("""WITH
    expanded_fixture AS (
        SELECT
            explode(response.fixture) as expanded_fixture,
            monotonically_increasing_id() as index_fixture 
        FROM
            df_fixtures
    )
             ,
    expanded_goals AS (
        SELECT
            explode(response.goals) expanded_goals,
            monotonically_increasing_id() AS index_goals
        FROM
            df_fixtures
    ),
    expanded_league AS (
        SELECT
            explode(response.league) as expanded_league,
            monotonically_increasing_id() AS index_league
        FROM
            df_fixtures
    ),
    expanded_teams AS (
        SELECT
            explode(response.teams) as expanded_teams,
            monotonically_increasing_id() AS index_teams
        FROM
            df_fixtures),
    df_fixture_goals AS (
        SELECT
            expanded_fixture.*,
            expanded_goals.*
        FROM
            expanded_fixture
            JOIN expanded_goals ON expanded_fixture.index_fixture = expanded_goals.index_goals
    ),
    df_league_fixture AS (
        SELECT
            df_fixture_goals.*,
            expanded_league.*
        FROM
            df_fixture_goals
            JOIN expanded_league ON df_fixture_goals.index_fixture = expanded_league.index_league
    ),
    df_team_fixture AS (
        SELECT
            df_league_fixture.*,
            expanded_teams.*
        FROM
            df_league_fixture
            JOIN expanded_teams ON df_league_fixture.index_fixture = expanded_teams.index_teams
    )
    Select
        df_team_fixture.expanded_fixture.date as date,
        df_team_fixture.expanded_fixture.id AS fixture_id,
        df_team_fixture.expanded_fixture.periods.first AS periods_first,
        df_team_fixture.expanded_fixture.periods.second AS periods_second,
        df_team_fixture.expanded_fixture.timestamp AS timestamp,
        df_team_fixture.expanded_fixture.referee AS referee,
        df_team_fixture.expanded_fixture.venue.id AS venue_id,
        df_team_fixture.expanded_goals.home AS home,
        df_team_fixture.expanded_goals.away AS away,
        df_team_fixture.expanded_league.id AS league_id,
        df_team_fixture.expanded_league.round AS round,
        df_team_fixture.expanded_league.season AS season,
        df_team_fixture.expanded_teams.home.id AS home_team_id,
        df_team_fixture.expanded_teams.away.id AS away_team_id       
    FROM
        df_team_fixture;
    
    """)
    
    return df_fixture_info

