# Databricks notebook source
spark
#spark Session


# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, BooleanType, DateType, DecimalType


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Create spark Session
spark=SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

ball_by_Ball_Schema=StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", IntegerType(), True),
    StructField("team_bowling", IntegerType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

#read tha Data


ball_by_ball=spark.read.schema(ball_by_Ball_Schema).format("csv").option("header","true").option("inferschema","true").load("s3://ipltill17/Ball_By_Ball.csv")


# COMMAND ----------

ball_by_ball.show(5)

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),  # PySpark doesn't have a specific YearType, so we use IntegerType
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])


Match=spark.read.schema(match_schema).format("CSV").option("header","true").load("s3://ipltill17/Match.csv")

# COMMAND ----------

Match.show(5)

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])


Player=spark.read.schema(player_schema).format("CSV").option("header","true").load("s3://ipltill17/Player.csv")

# COMMAND ----------

Player.show(5)

# COMMAND ----------


player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),  # PySpark does not have a YearType, so IntegerType is used
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

Player_Match=spark.read.schema(player_match_schema).format("CSV").option("header","true").load("s3://ipltill17/Player_match.csv")

# COMMAND ----------

Player_Match.show(5)

# COMMAND ----------


team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

team=spark.read.schema(team_schema).format("CSV").option("header","true").load("s3://ipltill17/Team.csv")

# COMMAND ----------

team.show(5)

# COMMAND ----------

#filter to include only valid deliveries (excluding extra like wides and no balls for specific analyses  )
ball_by_ball=ball_by_ball.filter((col("Wides")==0) & (col("noballs")==0))

#Aggrigation: Calculate the total and average runs scored in each match and innings

total_and_avg_rum=ball_by_ball.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("Total_Run"),
    avg("runs_scored").alias("Avg_Run"))



# COMMAND ----------

#Window function: calculate running total of run in each match and for each over
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

window=Window.partitionBy("Match_id","innings_no").orderBy("over_id")

ball_by_ball=ball_by_ball.withColumn(
"Running_Total_Run",
sum("runs_scored").over(window))



# COMMAND ----------

ball_by_ball=ball_by_ball.filter((col("Wides")==0) & (col("noballs")==0))



# COMMAND ----------

ball_by_ball=ball_by_ball.withColumn(
    "high_impact",
    when((col("runs_scored")+col("extra_runs")>6)| (col("bowler_wicket")==True),True).otherwise(False)
)

# COMMAND ----------

ball_by_ball.show(5)

# COMMAND ----------

#Exteacting Year, Month and Day from match_date for more detailed time-base analyses
Match=Match.withColumn("year", year("match_date"))
Match=Match.withColumn("Month", month("match_date"))
Match=Match.withColumn("Day", dayofmonth("match_date"))

#High Margin win: Categorizing win margin into high,medium and low
Match=Match.withColumn(
    "Win-Mergin-Category",
    when(col("win_margin")>=100,"High")
    .when((col("win_margin")>=50) & (col("win_margin")<100),"Medium")
    .otherwise("low"))
#Analyze the impact toss: who win the tose and match
Match=Match.withColumn(
    "toss_Match_Winner",
    when(col("toss_winner")==col("match_winner"),"yess").otherwise("No")
                       )
    #Show the enhanced match DataFrame
Match.show()


# COMMAND ----------

#Normalize and clean player name

Player=Player.withColumn("Player_Name",lower(regexp_replace("player_name","[^a-zA-Z0-9]","")))

#Hendling missing values in batting_hand and bowling_skill with a default 'unknown'
Player=Player.na.fill({'batting_hand':'unknown','bowling_skill':'unknown'})

#Categorized players based on batting hand
Player=Player.withColumn(
    'Batting-style',
    when(col("batting_hand").contains("left"),"Left-Handed").otherwise("Right-Handed")
                           )
#Show the modified dateframe

Player.show(2)



# COMMAND ----------

# add veteran_status column based on players age

Player_Match=Player_Match.withColumn(
    "Veteran_Status",
    when(col("age_as_on_match")>=35,"Veteran").otherwise("Non-Veteran"))


#Dynamic column to calculate year since debut 

Player_Match=Player_Match.withColumn(
    "year_since_debut",
    (year(current_date())-col("season_year")))

#show Reasult

Player_Match.show()

                                    

