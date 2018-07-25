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

df2 = spark2.read.csv("NBACleanData/SalaryClean.csv", header=True)
df2.createOrReplaceTempView("salary")

df_salary = spark.sql("select distinct trim(upper(sal.team)) as team, sal.team_name,  sum(sal.salary) as SalaryExpense from salary sal group by trim(upper(sal.team)), sal.team_name")

df_stats = spark.sql("SELECT trim(upper(st.tm)) as tm, sum(PTS) as pts from stats st  group by trim(upper(st.tm))")

#df_stats.show(30)
#
#df_salary.show(30)

df_inner = df_salary.join(df_stats, (df_salary.team == df_stats.tm))
df_inner.createOrReplaceTempView("positionVsalary")
df_inner = spark.sql("Select team_name, sum(SalaryExpense) as total_expense, sum(Pts) as points from positionVsalary group by team_name")
#df_inner.show(500)


pandasDF = df_inner.toPandas()


pandasDF.to_csv('NBACleanData/PositionTeamVSalary.csv', index=False)

