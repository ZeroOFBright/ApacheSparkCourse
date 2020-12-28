from pyspark import SparkConf,SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf = conf)

def normalizeWord(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

input = sc.textFile("file:///sparkCourse/book.txt")
word = input.flatMap(normalizeWord)
wordCount = word.map(lambda x:(x,1)).reduceByKey(lambda x,y : x + y)
wordCount = wordCount.map(lambda x: (x[1],x[0]) ).sortByKey()
results  = wordCount.collect()

for result in results:
    num = str(result[0])
    word = result[1].encode('ascii',"ignore")
    if(word):
        print(word.decode(),num)
