from pyspark.sql import SparkSession,Row
import collections
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    field = line.split(",")
    return Row(ID=int(field[0]),name=str(field[1].encode("utf-8")),age=int(field[2]),numFriends=int(field[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")
 
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()