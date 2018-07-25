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

#df2.show()

#df_inner = df.join(df2, (df.Tm == df2.team) & (df.)

#df_inner.createOrReplaceTempView("joined")

#df_final = spark.sql("SELECT team_name, SUM(PTS) as total_points FROM (Select * From joined where year Between 2017 AND 2018) Group by team_name")
df_salary = spark.sql("select distinct sal.team_name, trim(upper(sal.team)) as team from salary sal")
#df_hardik = spark.sql("select (select distinct sal.team_name from salary sal where sal.team = st.tm) as team_name, tm, year, pts from (SELECT st.tm, st.year, sum(PTS) as pts from stats st where st.year between 2017 and 2018 and st.tm = 'GSW' group by st.tm, st.year) q")

df_stats = spark.sql("SELECT  st.tm ,st.year, sum(PTS) as pts from stats st group by st.year, st.tm")
#df_hardik = spark.sql("SELECT team_name, st.year, trim(upper(sal.team)), sum(PTS) from stats st join salary sal on (sal.team = st.tm and st.year between season_start and  sal.season_end) where st.year between 2017 and 2018 group by team_name, st.year")

df_inner = df_salary.join(df_stats, (df_salary.team == df_stats.tm))
df_inner.createOrReplaceTempView("teamVpoints")

#df_finals = df_final.sort(desc("total_points"))
#df_stats.show(50)
#df_salary.show(20)
df_final = spark.sql("Select team_name, SUM(pts) From teamVpoints group by team_name")
df_final.show(30)
#df = pd.read_csv("NBACleanData/StatsClean.csv")
#
#df2 = pd.read_csv("NBACleanData/SalaryClean.csv")
#df2 = df2.rename(columns={'team': 'Tm'})
##print (df2.head())
#df3 = df.merge(df2, on="Tm", how="inner")
#df3 = df3[['team_name', 'PTS']]
##print(df3[['team_name','PTS']])
#print(df3.groupby(['team_name']).mean())

pandasDF = df_final.toPandas()
pandasDF.rename(columns={'sum(pts)': 'Points'}, inplace=True)

print (pandasDF.head())

pandasDF.to_csv('NBACleanData/TeamPointsAlltime.csv', index=False)
