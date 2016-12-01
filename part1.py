import sys
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("YELP").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

businesses = sys.argv[1]

# TODO
# CONVERT BOTH TO RDDs
# GroupBy on the reviews RDD by business_id
# join by business_id
# write that RDD to out

df = sqlContext.read.json(businesses)

reviews = sys.argv[2] # reviews

df2 = sqlContext.read.json(reviews)

# name & categories

# group reviews by name/id

df2 = df2.select("business_id", "text").groupBy("business_id").pivot("business_id")

print df2

everything = df.join(df2, df.business_id == df2.business_id)

minimized = everything.select("name", "categories", "text")

minimized.show()

# Write to file

# minimized.rdd.saveAsTextFile("/Users/kevinc/Code/YelpChallenge2016/out")


# head -n 100 > tosmallerfile1
