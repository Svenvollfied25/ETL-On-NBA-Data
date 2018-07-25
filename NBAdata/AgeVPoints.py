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

df2 = spark.sql("select sum(PTS) as Points, Age From stats Where Age between 19 AND 40 group by Age order by Age")

df2.show()

pandasDF = df2.toPandas()


pandasDF.to_csv('NBACleanData/AgeVPoints.csv', index=False)
