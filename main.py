from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
#Q1
df = spark.read.csv("/home/akshatjain98/Downloads/olympic_athlete_events.csv", header=True, inferSchema=True)
df.show(5)
#Q2
df=df.filter(df['Name'].isNotNull())
#Q3
df=df.filter(df['Year']>2023)
df.show()
#Q4
from pyspark.sql.functions import count
gender_counts = df.groupBy("Year", "Sex").agg(count("*").alias("count"))
gender_counts= gender_counts.orderBy("Year")
gender_counts.show(10)


