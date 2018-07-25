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

df = spark.sql("Select Pts, First_Name, Last_Name from stats")

df2 = spark2.read.csv("NBACleanData/PlayersClean.csv", header=True)
df2.createOrReplaceTempView("players")



df_inner = df2.join(df, (df2.First_Name == df.First_Name) & (df2.Last_Name == df.Last_Name))
df_inner.createOrReplaceTempView("HeightVScore")
df_inner = spark.sql("Select height, SUM(Pts) as points From HeightVScore group by height")
df_inner.show(500)


pandasDF = df_inner.toPandas()


pandasDF.to_csv('NBACleanData/HeightVPoints.csv', index=False)