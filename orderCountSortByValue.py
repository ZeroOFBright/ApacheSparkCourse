from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("CountOrder")
sc = SparkContext(conf=conf)

file = sc.textFile("file:///SparkCourse/customer-orders.csv")

def readLine(line):
    field = line.split(",")
    customer_id = int(field[0])
    price = float(field[2])
    return (customer_id,price)

lines = file.map(readLine)
customerOrder = lines.reduceByKey(lambda x,y : x + y)
sortByValue = customerOrder.map(lambda x : (x[1],x[0])).sortByKey()
results = sortByValue.collect()

for result in results :
    
    print("customer ID : {} price {:.2f}".format(result[1],result[0]))
