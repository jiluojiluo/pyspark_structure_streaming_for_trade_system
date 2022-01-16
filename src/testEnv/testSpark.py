from pyspark.sql import SparkSession

logFile = "C:\spark-3.1.2-bin-hadoop3.2\python\README.md"
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Line with a:%i,lines with b:%i" %(numAs,numBs))

spark.stop()