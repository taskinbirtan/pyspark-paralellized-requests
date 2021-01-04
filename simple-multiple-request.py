from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession.builder.appName('SomeGreatApp').getOrCreate()
df = spark.read.csv("filePath", header=False, inferSchema= False)
sparkContext=spark.sparkContext
import requests
links = df.withColumn('links', format_string('/somelogicmaybe/%s', df['the-link-column'])).collect()
def getLink(link):
  r = requests.get(link])
  content = r.text
  # your logic goes here 

rdd = sparkContext.parallelize(links)
rdd.foreach(getLink)
