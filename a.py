from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("friends")
sc = SparkContext(conf = conf)

def readLine(line):
    context = line.split(",")
    age = int(context[2])
    friends = int(context[3])
    return (age,friends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(readLine)
totalFriend = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0] + y[0] , x[1] + y[1]))
avg = totalFriend.mapValues(lambda x: x[0]/x[1])
results = avg.collect()

for result in results :
    print(result)

