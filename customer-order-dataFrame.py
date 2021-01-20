from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType

spark = SparkSession.builder.appName("customerOrder").getOrCreate()

schema = StructType([StructField("ID",IntegerType(),True),
                    StructField("ITEM_ID",IntegerType(),True),
                    StructField("PRICE",FloatType(),True)])

df = spark.read.schema(schema).csv("file:///ApacheSparkCourse/customer-orders.csv")

customerBuy = df.select("ID","PRICE")
customerBuy = customerBuy.groupBy("ID").agg(func.round(func.sum("PRICE"),2).alias("TOTAL"))

customerBuy = customerBuy.sort("TOTAL")


customerBuy.show(customerBuy.count())

