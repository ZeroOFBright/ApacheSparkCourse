from pyspark.sql import SparkSession,column
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("fakefriends").getOrCreate()

people = spark.read.option("header","true").option("inferSchema","true").csv("file:///SparkCourse/fakefriends-header.csv")

ageAndFriends = people.select("age","friends")

ageAndFriends.groupBy("age").avg("friends").orderBy("age").show()

ageAndFriends.groupBy("age").agg(func.round(func.avg("friends"),2)).sort("age").show()



spark.stop()