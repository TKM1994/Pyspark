from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

rdd1 = spark.sparkContext.textFile("word_count.txt")
rdd2 = rdd1.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda word : (word,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)

#to save output file 
# rdd4.saveAsTextFile("C:\\Users\\TARUN\\Desktop\\Pyspark\\wordoutput")

result = rdd4.collect()


for word, count in result:
    print(f"{word}: {count}")

# Stop the SparkSession
spark.stop()