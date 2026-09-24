# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # IPL Data Engineering & Analytics Pipeline
# MAGIC **Tools:** Databricks, PySpark, Spark SQL, Matplotlib, Seaborn

# COMMAND ----------

# Cell 1 — Imports

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import (
    col, when, sum, avg, count, max, min, round as spark_round,
    row_number, dense_rank, year, month, trim, isnull, desc
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("IPL_Pipeline").getOrCreate()
spark

# COMMAND ----------

# Cell 2 — Define Schemas

ball_schema = StructType([
    StructField("Match_id", IntegerType(), True),
    StructField("Over_id", IntegerType(), True),
    StructField("Ball_id", IntegerType(), True),
    StructField("Innings_No", IntegerType(), True),
    StructField("Team_Batting", StringType(), True),
    StructField("Team_Bowling", StringType(), True),
    StructField("Striker_Batting_Position", IntegerType(), True),
    StructField("Extra_Type", StringType(), True),
    StructField("Runs_Scored", IntegerType(), True),
    StructField("Extra_runs", IntegerType(), True),
    StructField("Wides", IntegerType(), True),
    StructField("Legbyes", IntegerType(), True),
    StructField("Byes", IntegerType(), True),
    StructField("Noballs", IntegerType(), True),
    StructField("Penalty", IntegerType(), True),
    StructField("Bowler_Extras", IntegerType(), True),
    StructField("Out_type", StringType(), True),
    StructField("Caught", StringType(), True),
    StructField("Bowled", StringType(), True),
    StructField("Run_out", StringType(), True),
    StructField("LBW", StringType(), True),
    StructField("Retired_hurt", StringType(), True),
    StructField("Stumped", StringType(), True),
    StructField("caught_and_bowled", StringType(), True),
    StructField("hit_wicket", StringType(), True),
    StructField("ObstructingFeild", StringType(), True),
    StructField("Bowler_Wicket", StringType(), True),
    StructField("Match_Date", StringType(), True),
    StructField("Season", IntegerType(), True),
    StructField("Striker", IntegerType(), True),
    StructField("Non_Striker", IntegerType(), True),
    StructField("Bowler", IntegerType(), True),
    StructField("Player_Out", StringType(), True),
    StructField("Fielders", StringType(), True),
    StructField("Striker_match_SK", IntegerType(), True),
    StructField("StrikerSK", IntegerType(), True),
    StructField("NonStriker_match_SK", IntegerType(), True),
    StructField("NONStriker_SK", IntegerType(), True),
    StructField("Fielder_match_SK", IntegerType(), True),
    StructField("Fielder_SK", IntegerType(), True),
    StructField("Bowler_match_SK", IntegerType(), True),
    StructField("BOWLER_SK", IntegerType(), True),
    StructField("PlayerOut_match_SK", IntegerType(), True),
    StructField("BattingTeam_SK", IntegerType(), True),
    StructField("BowlingTeam_SK", IntegerType(), True),
    StructField("Keeper_Catch", StringType(), True),
    StructField("Player_out_sk", IntegerType(), True),
    StructField("MatchDateSK", StringType(), True),
])

match_schema = StructType([
    StructField("Match_SK", IntegerType(), True),
    StructField("Match_Id", IntegerType(), True),
    StructField("Team1", StringType(), True),
    StructField("Team2", StringType(), True),
    StructField("Match_Date", StringType(), True),
    StructField("Season_Year", IntegerType(), True),
    StructField("Venue_Name", StringType(), True),
    StructField("City_Name", StringType(), True),
    StructField("Country_Name", StringType(), True),
    StructField("Toss_Winner", StringType(), True),
    StructField("Match_Winner", StringType(), True),
    StructField("Toss_Name", StringType(), True),
    StructField("Win_Type", StringType(), True),
    StructField("Outcome_Type", StringType(), True),
    StructField("ManOfMatch", StringType(), True),
    StructField("Win_Margin", IntegerType(), True),
    StructField("Country_Id", IntegerType(), True),
])

player_schema = StructType([
    StructField("Player_SK", IntegerType(), True),
    StructField("Player_Id", IntegerType(), True),
    StructField("Player_Name", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("Batting_Hand", StringType(), True),
    StructField("Bowling_Skill", StringType(), True),
    StructField("Country_Name", StringType(), True),
])

player_match_schema = StructType([
    StructField("Player_match_SK", IntegerType(), True),
    StructField("PlayerMatch_key", StringType(), True),
    StructField("Match_Id", IntegerType(), True),
    StructField("Player_Id", IntegerType(), True),
    StructField("Player_Name", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("Batting_Hand", StringType(), True),
    StructField("Bowling_Skill", StringType(), True),
    StructField("Country_Name", StringType(), True),
    StructField("Role_Desc", StringType(), True),
    StructField("Player_Team", StringType(), True),
    StructField("Opposit_Team", StringType(), True),
    StructField("Season_Year", IntegerType(), True),
    StructField("Is_ManOfTheMatch", StringType(), True),
    StructField("Age_As_On_Match", IntegerType(), True),
    StructField("IsPlayers_Team_Won", StringType(), True),
    StructField("Batting_Status", StringType(), True),
    StructField("Bowling_Status", StringType(), True),
    StructField("Player_Captain", StringType(), True),
    StructField("Opposit_Captain", StringType(), True),
    StructField("Player_Keeper", StringType(), True),
    StructField("Opposit_Keeper", StringType(), True),
])

team_schema = StructType([
    StructField("Team_SK", IntegerType(), True),
    StructField("Team_Id", IntegerType(), True),
    StructField("Team_Name", StringType(), True),
])

print("Schemas defined")

# COMMAND ----------

# Cell 3 — EXTRACT: Load all datasets

ball_df = spark.read.csv("/FileStore/tables/Ball_By_Ball.csv", header=True, schema=ball_schema)
match_df = spark.read.csv("/FileStore/tables/Match.csv", header=True, schema=match_schema)
player_df = spark.read.csv("/FileStore/tables/Player.csv", header=True, schema=player_schema)
player_match_df = spark.read.csv("/FileStore/tables/Player_match.csv", header=True, schema=player_match_schema)
team_df = spark.read.csv("/FileStore/tables/Team.csv", header=True, schema=team_schema)

print(f"Ball by Ball : {ball_df.count()} rows")
print(f"Match        : {match_df.count()} rows")
print(f"Player       : {player_df.count()} rows")
print(f"Player Match : {player_match_df.count()} rows")
print(f"Team         : {team_df.count()} rows")

# COMMAND ----------

# Cell 4 — Explore raw data

display(ball_df)

# COMMAND ----------

display(match_df)

# COMMAND ----------

display(player_df)

# COMMAND ----------

display(player_match_df)

# COMMAND ----------

display(team_df)

# COMMAND ----------

# Cell 5 — TRANSFORM: Tag each ball with match phase (Powerplay / Middle / Death)

ball_df = ball_df.withColumn(
    "match_phase",
    when(col("Over_id") <= 6, "Powerplay")
    .when(col("Over_id") <= 15, "Middle")
    .otherwise("Death")
)

# Tag boundaries
ball_df = ball_df.withColumn(
    "is_boundary",
    when((col("Runs_Scored") == 4) | (col("Runs_Scored") == 6), True).otherwise(False)
)

# Tag dot balls
ball_df = ball_df.withColumn(
    "is_dot_ball",
    when((col("Runs_Scored") == 0) & (col("Extra_runs") == 0), True).otherwise(False)
)

ball_df.select("Over_id", "match_phase", "Runs_Scored", "is_boundary", "is_dot_ball").show(10)

# COMMAND ----------

# Cell 6 — TRANSFORM: Enrich match data

# Flag close matches
match_df = match_df.withColumn(
    "is_close_match",
    when(
        ((col("Win_Type") == "runs") & (col("Win_Margin") <= 10)) |
        ((col("Win_Type") == "wickets") & (col("Win_Margin") <= 2)),
        True
    ).otherwise(False)
)

# Did toss winner win the match?
match_df = match_df.withColumn(
    "toss_winner_won",
    when(col("Toss_Winner") == col("Match_Winner"), "Yes").otherwise("No")
)

match_df.select("Match_Id", "Match_Winner", "Win_Margin", "is_close_match", "toss_winner_won").show(10)

# COMMAND ----------

# Cell 7 — TRANSFORM: Classify players

player_df = player_df.na.fill({"Batting_Hand": "Unknown", "Bowling_Skill": "Unknown"})

player_df = player_df.withColumn(
    "player_type",
    when(col("Bowling_Skill").contains("fast") | col("Bowling_Skill").contains("medium"), "Pacer")
    .when(col("Bowling_Skill").contains("spin") | col("Bowling_Skill").contains("Legbreak") |
          col("Bowling_Skill").contains("Offbreak") | col("Bowling_Skill").contains("Slow"), "Spinner")
    .otherwise("Batsman/Unknown")
)

player_df = player_df.withColumn(
    "is_overseas",
    when(col("Country_Name") != "India", True).otherwise(False)
)

player_df.select("Player_Name", "Country_Name", "player_type", "is_overseas").show(10)

# COMMAND ----------

# Cell 8 — Register temp views for Spark SQL

ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")

print("All views registered")

# COMMAND ----------

# Cell 9 — ANALYSIS 1: Boundary percentage by match phase across seasons

boundary_by_phase = spark.sql("""
    SELECT m.Season_Year, b.match_phase,
           COUNT(*) AS total_balls,
           SUM(CASE WHEN b.is_boundary THEN 1 ELSE 0 END) AS boundaries,
           ROUND(SUM(CASE WHEN b.is_boundary THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS boundary_pct
    FROM ball_by_ball b
    JOIN match m ON b.Match_id = m.Match_Id
    GROUP BY m.Season_Year, b.match_phase
    ORDER BY m.Season_Year
""")
boundary_by_phase.show(30)

# COMMAND ----------

# Cell 10 — ANALYSIS 2: Best strike rate batsmen in death overs (min 100 balls)

death_batsmen = spark.sql("""
    SELECT p.Player_Name,
           COUNT(*) AS balls_faced,
           SUM(b.Runs_Scored) AS runs,
           ROUND(SUM(b.Runs_Scored) * 100.0 / COUNT(*), 2) AS strike_rate
    FROM ball_by_ball b
    JOIN player_match pm ON b.Match_id = pm.Match_Id AND b.Striker = pm.Player_Id
    JOIN player p ON pm.Player_Id = p.Player_Id
    WHERE b.match_phase = 'Death' AND b.Wides = 0
    GROUP BY p.Player_Name
    HAVING COUNT(*) >= 100
    ORDER BY strike_rate DESC
""")
death_batsmen.show(15)

# COMMAND ----------

# Cell 11 — ANALYSIS 3: Best dot ball percentage bowlers (min 200 balls)

dot_ball_bowlers = spark.sql("""
    SELECT p.Player_Name,
           COUNT(*) AS balls_bowled,
           SUM(CASE WHEN b.is_dot_ball THEN 1 ELSE 0 END) AS dot_balls,
           ROUND(SUM(CASE WHEN b.is_dot_ball THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS dot_ball_pct
    FROM ball_by_ball b
    JOIN player_match pm ON b.Match_id = pm.Match_Id AND b.Bowler = pm.Player_Id
    JOIN player p ON pm.Player_Id = p.Player_Id
    GROUP BY p.Player_Name
    HAVING COUNT(*) >= 200
    ORDER BY dot_ball_pct DESC
""")
dot_ball_bowlers.show(15)

# COMMAND ----------

# Cell 12 — ANALYSIS 4: Season-wise team win percentage

team_wins = spark.sql("""
    SELECT Season_Year, Team,
           COUNT(*) AS played,
           SUM(CASE WHEN Team = Match_Winner THEN 1 ELSE 0 END) AS wins,
           ROUND(SUM(CASE WHEN Team = Match_Winner THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS win_pct
    FROM (
        SELECT Match_Id, Season_Year, Team1 AS Team, Match_Winner FROM match
        UNION ALL
        SELECT Match_Id, Season_Year, Team2 AS Team, Match_Winner FROM match
    )
    GROUP BY Season_Year, Team
    HAVING COUNT(*) >= 5
    ORDER BY Season_Year, win_pct DESC
""")
team_wins.show(50)

# COMMAND ----------

# Cell 13 — ANALYSIS 5: Toss decision effectiveness — bat vs field by venue

toss_effectiveness = spark.sql("""
    SELECT Venue_Name, Toss_Name AS decision,
           COUNT(*) AS matches,
           SUM(CASE WHEN Toss_Winner = Match_Winner THEN 1 ELSE 0 END) AS wins,
           ROUND(SUM(CASE WHEN Toss_Winner = Match_Winner THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS win_pct
    FROM match
    WHERE Toss_Name IS NOT NULL
    GROUP BY Venue_Name, Toss_Name
    HAVING COUNT(*) >= 5
    ORDER BY win_pct DESC
""")
toss_effectiveness.show(20)

# COMMAND ----------

# Cell 14 — ANALYSIS 6: Captain win rates

captain_stats = spark.sql("""
    SELECT Player_Name AS captain,
           COUNT(DISTINCT Match_Id) AS matches,
           SUM(CASE WHEN IsPlayers_Team_Won = 'TRUE' THEN 1 ELSE 0 END) AS wins,
           ROUND(SUM(CASE WHEN IsPlayers_Team_Won = 'TRUE' THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT Match_Id), 2) AS win_pct
    FROM player_match
    WHERE Player_Name = Player_Captain
    GROUP BY Player_Name
    HAVING COUNT(DISTINCT Match_Id) >= 20
    ORDER BY win_pct DESC
""")
captain_stats.show(15)

# COMMAND ----------

# Cell 15 — ANALYSIS 7: Close matches per season

close_trend = spark.sql("""
    SELECT Season_Year,
           COUNT(*) AS total,
           SUM(CASE WHEN is_close_match THEN 1 ELSE 0 END) AS close,
           ROUND(SUM(CASE WHEN is_close_match THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS close_pct
    FROM match
    WHERE Outcome_Type = 'Result'
    GROUP BY Season_Year
    ORDER BY Season_Year
""")
close_trend.show()

# COMMAND ----------

# Cell 16 — ANALYSIS 8: Overseas vs domestic player performance

overseas_impact = spark.sql("""
    SELECT p.is_overseas,
           COUNT(*) AS balls,
           SUM(b.Runs_Scored) AS total_runs,
           ROUND(SUM(b.Runs_Scored) * 100.0 / COUNT(*), 2) AS strike_rate,
           ROUND(SUM(CASE WHEN b.is_boundary THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS boundary_pct
    FROM ball_by_ball b
    JOIN player_match pm ON b.Match_id = pm.Match_Id AND b.Striker = pm.Player_Id
    JOIN player p ON pm.Player_Id = p.Player_Id
    WHERE b.Wides = 0
    GROUP BY p.is_overseas
""")
overseas_impact.show()

# COMMAND ----------

# Cell 17 — VISUALIZATION 1: Boundary % by phase across seasons

import matplotlib.pyplot as plt
import seaborn as sns

bp_pd = boundary_by_phase.toPandas()
plt.figure(figsize=(14, 6))
sns.lineplot(data=bp_pd, x='Season_Year', y='boundary_pct', hue='match_phase', marker='o', linewidth=2)
plt.title('Boundary Percentage by Match Phase Across Seasons', fontsize=14, fontweight='bold')
plt.xlabel('Season')
plt.ylabel('Boundary %')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# Cell 18 — VISUALIZATION 2: Death overs strike rate — top batsmen

death_pd = death_batsmen.limit(12).toPandas()
plt.figure(figsize=(12, 6))
bars = plt.barh(death_pd['Player_Name'], death_pd['strike_rate'], color='coral', edgecolor='black')
plt.xlabel('Strike Rate')
plt.title('Best Death Over Batsmen by Strike Rate (Min 100 Balls)', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()
for bar, val in zip(bars, death_pd['strike_rate']):
    plt.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, f'{val}', va='center', fontsize=9)
plt.tight_layout()
plt.show()

# COMMAND ----------

# Cell 19 — VISUALIZATION 3: Dot ball bowlers

dot_pd = dot_ball_bowlers.limit(12).toPandas()
plt.figure(figsize=(12, 6))
plt.barh(dot_pd['Player_Name'], dot_pd['dot_ball_pct'], color='steelblue', edgecolor='black')
plt.xlabel('Dot Ball %')
plt.title('Best Dot Ball Bowlers (Min 200 Balls)', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# COMMAND ----------

# Cell 20 — VISUALIZATION 4: Team win percentage heatmap

team_pd = team_wins.toPandas()
pivot = team_pd.pivot_table(index='Team', columns='Season_Year', values='win_pct')
plt.figure(figsize=(16, 9))
sns.heatmap(pivot, annot=True, fmt='.0f', cmap='RdYlGn', linewidths=0.5, cbar_kws={'label': 'Win %'})
plt.title('Team Win Percentage by Season', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# COMMAND ----------

# Cell 21 — VISUALIZATION 5: Captain win rates

cap_pd = captain_stats.toPandas()
plt.figure(figsize=(12, 6))
colors = ['green' if x >= 55 else 'red' for x in cap_pd['win_pct']]
plt.barh(cap_pd['captain'], cap_pd['win_pct'], color=colors, edgecolor='black')
plt.axvline(x=50, color='black', linestyle='--', linewidth=1)
plt.xlabel('Win %')
plt.title('Captain Win Rates (Min 20 Matches)', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# COMMAND ----------

# Cell 22 — VISUALIZATION 6: Close match trend

close_pd = close_trend.toPandas()
fig, ax1 = plt.subplots(figsize=(12, 5))
ax1.bar(close_pd['Season_Year'], close_pd['total'], color='lightgray', edgecolor='black', label='Total')
ax1.bar(close_pd['Season_Year'], close_pd['close'], color='tomato', edgecolor='black', label='Close')
ax1.set_ylabel('Matches')
ax1.legend(loc='upper left')

ax2 = ax1.twinx()
ax2.plot(close_pd['Season_Year'], close_pd['close_pct'], color='navy', marker='o', linewidth=2)
ax2.set_ylabel('Close Match %', color='navy')

plt.title('Close Match Trends Across Seasons', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# COMMAND ----------

# Cell 23 — VISUALIZATION 7: Toss decision effectiveness — top venues

toss_pd = toss_effectiveness.limit(16).toPandas()
plt.figure(figsize=(13, 7))
sns.barplot(data=toss_pd, x='win_pct', y='Venue_Name', hue='decision', palette='Set2', edgecolor='black')
plt.xlabel('Win % After Winning Toss')
plt.title('Bat or Field? Toss Effectiveness by Venue', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# COMMAND ----------

# Cell 24 — LOAD: Save enriched data as Delta tables

ball_df.write.format("delta").mode("overwrite").saveAsTable("ipl_ball_enriched")
match_df.write.format("delta").mode("overwrite").saveAsTable("ipl_match_enriched")
player_df.write.format("delta").mode("overwrite").saveAsTable("ipl_player_enriched")
player_match_df.write.format("delta").mode("overwrite").saveAsTable("ipl_player_match_enriched")
team_df.write.format("delta").mode("overwrite").saveAsTable("ipl_team")

print("All tables saved to Delta Lake!")
spark.sql("SHOW TABLES").show()

# COMMAND ----------

# Cell 25 — Verify: Read back from Delta

print(f"ball_enriched: {spark.table('ipl_ball_enriched').count()} rows")
print(f"match_enriched: {spark.table('ipl_match_enriched').count()} rows")
print("Pipeline complete!")
