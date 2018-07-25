from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("Python NBA Salaries") \
    .getOrCreate()


df = spark.read.csv("NBAdata/nba_salaries_1990_to_2018.csv", header=True)
df = df.fillna("Unknown")

df.createOrReplaceTempView("salaries")

split_col = F.split(df['player'], ' ')
df2 = df.withColumn('First_Name', split_col.getItem(0))
df2 = df2.withColumn('Last_Name', split_col.getItem(1))

#spark.sql("SELECT player FROM salaries WHERE team IS NULL").show()


df2.show(10)

### Code to convert RDD into Pandas
pandasDF = df2.toPandas()
print(pandasDF.head(10))


pandasDF.to_csv('NBACleanData/SalaryClean.csv', index=False)

#try:
#    df2.write.option("header","true").format("com.databricks.spark.csv").csv("NBACleanData/") \
#    .save(path="CleanNBASalary.csv", mode='overwrite')
#except AttributeError:
#    print ("Some Error")
#        