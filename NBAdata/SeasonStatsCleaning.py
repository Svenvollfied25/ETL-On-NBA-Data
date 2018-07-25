from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd


spark = SparkSession \
    .builder \
    .appName("Python NBA Salaries") \
    .getOrCreate()


df = spark.read.csv("NBAdata/Seasons_stats.csv", header=True)
df.drop('blanl').collect()
df.drop('blank2').collect()

df.createOrReplaceTempView("stats")
#spark.sql("SELECT count(*) FROM stats").show()

#sqlDF = spark.sql("DELETE FROM stats WHERE Player IS NULL")

## code to remove rows with empty player names
df2 = df.na.drop(how='any', subset=['Player','Age'])
#spark.sql("SELECT count(*) FROM stats").show()
df2.createOrReplaceTempView("stats")
#sqlDF = df.na.drop(["Players"])
spark.sql("SELECT Age, Player FROM stats WHERE Age IS NULL").show()
#spark.sql("SELECT count(*) FROM stats").show()

# Code to split Player names into First Name and Last Name
split_col = F.split(df2['Player'], ' ')
df2 = df2.withColumn('First_Name', split_col.getItem(0))
df2 = df2.withColumn('Last_Name', split_col.getItem(1))

# code to check null to not null ratio to decide wether or not drop the column
#sqlDF = spark.sql("SELECT count(*) FROM stats WHERE GS IS NULL \
#UNION ALL \
#SELECT count(*) FROM stats WHERE GS IS NOT NULL")
#df2.show()


### Code to convert RDD into Pandas
pandasDF = df2.toPandas()
print(pandasDF.head(10))

pandasDF.to_csv('NBACleanData/StatsClean.csv', index=False)


#try:
#    df2.write.option("header","true").format("com.databricks.spark.csv").csv("NBACleanData/") \
#    .save(path="StatsCleaning.csv", mode='overwrite')
#except AttributeError:
#    print ("Some Error")
#        

