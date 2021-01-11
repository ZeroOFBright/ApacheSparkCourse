from pyspark.sql import SparkSession,Row
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header","true").option("inferSchema","true").csv("file:///SparkCourse/fakefriends-header.csv")
print("inferred schema")
people.printSchema()

print("name column")
people.select("name").show()

print("num friends column")
people.select("friends").show()

print("filter people age more than 21")
people.filter(people.age>21).show()

print("filter people friends more than 300")
people.filter(people.friends>300).show()

print("group by age")
people.groupBy("age").count().show()

print("group by num friends")
people.groupBy("friends").count().show()

print("+ 10 year old")
people.select(people.name,people.age+10).show()

spark.stop()