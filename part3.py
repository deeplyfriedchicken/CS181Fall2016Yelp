from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkConf, SparkContext
from pyspark.mllib.classification import SVMWithSGD, SVMModel
import unicodedata
import sys

# Prof. Salloum Code!
########################################
def parseLine(line):
	parts = line.split(";")
	name = parts[0].encode('ascii','ignore')
	categories = [x.strip() for x in parts[1].split(",") ]
	reviews = parts[2].encode('ascii','ignore')
	#  (name , category , reviews-text)
	return (name, categories[0], reviews)


conf = SparkConf().setAppName("YELP").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

# Load documents (one per line).
documents = sc.textFile(sys.argv[1]).map(parseLine)

label = documents.map(lambda x: x[1])
features = documents.map(lambda x: x[2])

labelSet = list(set(label.collect())) # change RDD to set (only unique categories)
print "Category-Label mapping:", labelSet

hashingTF = HashingTF(5000)
tf = hashingTF.transform(features)

tf.cache()
idf = IDF(minDocFreq=5).fit(tf)
tfidf = idf.transform(tf).cache()


data = label.zip(tfidf).map(lambda x: LabeledPoint(labelSet.index(x[0]), x[1])).cache()
training = data.sample(False, .90)
test = data.sample(False, .10)
print "Num Points:", data.count()
# Build the model
model = LogisticRegressionWithLBFGS.train(training, numClasses=len(labelSet))

# test a few items
labelsAndPreds = test.map(lambda p: (labelSet[int(p.label)], p.label,model.predict(p.features) ))
temp = labelsAndPreds.take(20)


for index in range(len(temp)):
	realVal = int(temp[0][1]) # typecasting real value from float to int so we can compare to predictedVal
	predictedVal = int(temp[0][2])

	# if (realVal == predictedVal):
	# 	#output = ([x[1] for x in labelsAndPreds],true)
	# 	print "true"
	# else:
	# 	#output = ([x[1] for x in labelsAndPreds],false)
	# 	print "false"

	accurate = 0
	inaccurate = 0
	if (realVal == predictedVal):
		accurate + 1
	else:
		inaccurate + 1

if (inaccurate == 0):
	final = 100
else:
	final = (accurate / (accurate + inaccurate)) * 100

print "Percent accurate:",final,"%"
