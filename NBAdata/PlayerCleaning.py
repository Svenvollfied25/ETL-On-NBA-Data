from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("Python NBA Salaries") \
    .getOrCreate()


df = spark.read.csv("NBAdata/Players.csv", header=True)
df = df.fillna("Unknown")

df.createOrReplaceTempView("players")
spark.sql("SELECT count(*) FROM players").show()

#sqlDF = spark.sql("DELETE FROM stats WHERE Player IS NULL")

## code to remove rows with empty player names
df2 = df.na.drop(how='any', subset=['Player'])

spark.sql("SELECT count(*) FROM players").show()


# Code to split Player names into First Name and Last Name
split_col = F.split(df2['Player'], ' ')
df2 = df2.withColumn('First_Name', split_col.getItem(0))
df2 = df2.withColumn('Last_Name', split_col.getItem(1))

df2.show(10)

### Code to convert RDD into Pandas
pandasDF = df2.toPandas()
print(pandasDF.head(10))


pandasDF.to_csv('NBACleanData/PlayersClean.csv', index=False)


#try:
#    df2.write.option("header","true").format("com.databricks.spark.csv").csv("NBACleanData/") \
#    .save(path="CleanPlayer.csv", mode='overwrite')
#except AttributeError:
#    print ("Some Error")
#        
