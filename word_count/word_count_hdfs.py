from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

#read data from hdfs
rdd1 = spark.sparkContext.textFile("hdfs://localhost:9000/user/tkm/data/word_count.txt")  
# Replace localhost:9000 with your HDFS address
rdd2 = rdd1.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda word : (word,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)

# To print
#result = rdd4.collect()
#for word, count in result:
#    print(f"{word}: {count}")

#to store in hdfs    
rdd4.collect()    
rdd4.saveAsTextFile("hdfs://localhost:9000/user/tkm/data/word_count_output")


# Stop the SparkSession
spark.stop()
