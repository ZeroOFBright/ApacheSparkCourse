from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType

spark = SparkSession.builder.appName("popularmovie").getOrCreate()

schema = StructType([StructField("userID",IntegerType(),True),
                    StructField("movieID",IntegerType(),True),
                    StructField("rating",IntegerType(),True),
                    StructField("timestamp",FloatType(),True)])

movieDF = spark.read.option("sep","\t").schema(schema).csv("file:///ApacheSparkCourse/ml-100k/u.data")

mostReview = movieDF.groupBy("movieID").count().orderBy(func.desc("count"))

mostReview.show(10)

spark.stop()

