from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd


spark = SparkSession \
    .builder \
    .appName("Python NBA Salaries") \
    .getOrCreate()

df = spark.read.csv("NBACleanData/SalaryClean.csv", header=True)

df = df.withColumn("salary", df["salary"].cast("double"))
df.printSchema()
df.createOrReplaceTempView("salary")

#df.show(10)
#sqlDF = df.na.drop(["Players"])
df = spark.sql("SELECT team_name, SUM(salary) AS wage_expenditure FROM salary WHERE season_end Between 2013 AND 2018 GROUP BY team_name")

pandasDF = df.toPandas()
pandasDF.rename(columns={'SUM(salary)': 'wage_expenditure'}, inplace=True)


pandasDF.to_csv('NBACleanData/TeamExpenditureLastFiveYears.csv', index=False)
