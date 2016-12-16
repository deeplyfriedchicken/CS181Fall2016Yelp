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

businesses = sqlContext.read.json(businesses)

reviews = sys.argv[2] # reviews

reviews = sqlContext.read.json(reviews)

# name & categories

# group reviews by name/id

# print "NEXT\n"
# print businesses.take(1)
#
# newRDD = reviews.join(businesses)
# print newRDD.take(3)
# print newRDD.count()



reviews = reviews.select("business_id", "text")
print reviews.take(1)
# reviews = reviews.groupBy(lambda value: value[0]).toDF(['business_id', 'text'])

# reviews = reviews.groupBy(lambda x: x = "business_id")
#
everything = businesses.join(reviews, businesses.business_id == reviews.business_id)

minimized = everything.select("name", "categories", "text")

minimized.show()

# Write to file

minimized.rdd.saveAsTextFile("/Users/kevinc/Code/YelpChallenge2016/out")


# head -n 100 > tosmallerfile1
