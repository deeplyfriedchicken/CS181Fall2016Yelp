import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import sys

conf = SparkConf().setAppName("YELP").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

data = sys.argv[1]

// Load documents (one per line).
val documents: RDD[Seq[String]] = sc.textFile(data)
  .map(_.split(" ").toSeq)

val hashingTF = new HashingTF()
val tf: RDD[Vector] = hashingTF.transform(documents)

// While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
// First to compute the IDF vector and second to scale the term frequencies by IDF.
tf.cache()
val idf = new IDF().fit(tf)
val tfidf: RDD[Vector] = idf.transform(tf)
