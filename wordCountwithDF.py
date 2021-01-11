from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

dataFrame = spark.read.text("file:///SparkCourse/book.txt")

words = dataFrame.select(func.explode(func.split(dataFrame.value,"\\W++")).alias("word"))
words.filter(words.word != "")

words = words.select(func.lower(words.word).alias("word"))

words = words.groupBy("word").count()

wordsSource = words.sort("count")

wordsSource.show(wordsSource.count())

