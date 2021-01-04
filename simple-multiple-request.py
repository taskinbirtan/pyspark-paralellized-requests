from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession.builder.appName('SomeGreatApp').getOrCreate()
df = spark.read.csv("filePath", header=False, inferSchema= False)
sparkContext=spark.sparkContext
import requests

def getLink(link):
  print(link])
  r = requests.get(link])
  content = r.text

rdd = sparkContext.parallelize(links)
rdd.foreach(getLink)