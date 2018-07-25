from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.functions import desc


spark = SparkSession \
    .builder \
    .appName("Python NBA Salaries") \
    .getOrCreate()

df = spark.read.csv("NBACleanData/StatsClean.csv", header=True)
df.createOrReplaceTempView("stats")

df2 = spark.sql("SELECT Pos, SUM(PTS) AS total_points FROM stats GROUP BY Pos")

df2 = df2.sort(desc("total_points"))
df2.show()

pandasDF = df2.toPandas()
pandasDF.rename(columns={'total_points': 'Points'},inplace=True)


pandasDF.to_csv('NBACleanData/AlltimeScoringPos.csv', index=False)

