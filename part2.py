import sys

textFile = sys.argv[1]
fileOut = open("test", "w")
count = 0
with open(textFile, 'r') as f:
    while True:
        line = f.readline()
        if(line.find("name=u'") != -1):
            index1 = line.find("name=u") + len("name=u") +1
            index2 = line.find("',")
        else:
            index1 = line.find("name=u") + len("name=u") +1
            index2 = line.find('",')
        index3 = line.find(", categories=[") + len(", categories=[")
        index4 = line.find("],")
        if(line.find("text=u'") != -1):
            index5 = line.find(" text=u'") + len(" text=u'")
            index6 = line.find("\n") # just need -2
        else:
            index5 = line.find(' text=u"') + len(' text=u"')
            index6 = line.find("\n") # just need -2
        name = line[index1:index2]
        categories = line[index3:index4-1].replace("u'", "").replace("' ", "")
        categories = categories.split(",")
        cat_count = 0
        for value in categories:
            print value
            if(value[len(value)-1:] == "'"):
                categories[cat_count] = value[0:len(value)-1]
                value = value[0:len(value)-1]
            if(value[0:1] == " "):
                categories[cat_count] = value[1:]
            print categories[cat_count]
            cat_count += 1
        review = line[index5:len(line)-3]
        print name
        print categories
        if not line: break
        fileOut.write(name + ";"+ str(categories) + ";" + review + "\n")
fileOut.close()


"""

# (name+category => key   ,  review => value)
def parseData(line):
    partsList = line.split(";")
    return (partsList[0]+";"partsList[1] ,  (partsList[2])  )

def reformatTuple(t):
    parts = t[0].split(";")   # key
    reviewsStr = " ".join(t[1])
    categoryList = parts[1][1:-1].replace("'", "").split(,)
    for category in categoryList :
        return (parts[0], category, reviewsStr)


dataRDD = sc.textFile(filename).map(parseData)


groupedRDD = dataRDD.groupByKey()   # (name+category  , list of reviews   )


# tuple of (name, category, reviewsList  )
dataRDDformated = groupedRDD.map(lambda x: reformatTuple(x))
reviewsRDD = dataRDDformated.map(lambda x: x[2])
hashingTF = HashingTF()
tf = hashingTF.transform(reviewsRDD)

# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
# First to compute the IDF vector and second to scale the term frequencies by IDF.
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

zip
"""
