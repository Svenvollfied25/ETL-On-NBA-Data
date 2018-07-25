from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.functions import desc


spark = SparkSession \
    .builder \
    .appName("Python NBA Salaries") \
    .getOrCreate()

spark2 = SparkSession \
    .builder \
    .appName("Python NBA Salaries") \
    .getOrCreate()
    
df = spark.read.csv("NBACleanData/StatsClean.csv", header=True)
df.createOrReplaceTempView("stats")

df2 = spark2.read.csv("NBACleanData/PlayersClean.csv", header=True)
df2.createOrReplaceTempView("players")


df_players = spark.sql("select birth_city, birth_state, born, trim(upper(First_Name)) as First_Name, trim(upper(Last_Name)) as Last_Name From players Where lower(birth_city) Not Like '%unknown%' OR lower(birth_state) Not Like '%unknown%'")


df_stats = spark.sql("SELECT trim(upper(First_Name)) as First_Name_stats, trim(upper(Last_Name)) as Last_Name_stats, Pos From stats")


df_inner = df_players.join(df_stats, (df_players.First_Name == df_stats.First_Name_stats) & (df_players.Last_Name == df_stats.Last_Name_stats))
df_inner.createOrReplaceTempView("positionVcountry")

df_final = spark.sql("Select first_name, last_name, born, Pos, birth_city, birth_state From positionVcountry Where lower(birth_city) Not Like '%unknown%' OR lower(birth_state) Not Like '%unknown%'")
df_final.show(60)
pandasDF = df_final.toPandas()
pandasDF.to_csv('NBACleanData/PositionVCountry.csv', index=False)