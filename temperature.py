from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("stationTemp")
sc = SparkContext(conf = conf)

def parseLine(lines):
    field = lines.split(",")
    stationID = field[0]
    entryType = field[2]
    temperature = float(field[3]) * 0.1 * (9.0/5.0) + 32.0
    return (stationID,entryType,temperature)


lines = sc.textFile("file:///SparkCourse/1800.csv")
parseLines = lines.map(parseLine)

minTemps = parseLines.filter(lambda x: "TMIN" in x[1])
maxTemps = parseLines.filter(lambda x: "TMAX" in x[1])

stationMinTemp = minTemps.map(lambda x : (x[0],x[2]))
stationMaxTemp = maxTemps.map(lambda x : (x[0],x[2]))

minTemps = stationMinTemp.reduceByKey(lambda x,y: min(x,y))
maxTemps = stationMaxTemp.reduceByKey(lambda x,y: max(x,y))
minResults = minTemps.collect()
maxResults = maxTemps.collect()
for result in minResults :
    print("Min temperature of " + result[0] + " temperature is {:.2f} F".format(result[1])) 
for result in maxResults :
    print("Max temperature of " + result[0] + " temperature is {:.2f} F".format(result[1])) 