from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("job_stage_task").getOrCreate()

somato_rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/tkm/data/somato.txt")


menu_rdd = somato_rdd.map(lambda x : (x.split(",")[4] , 1))

result = menu_rdd.reduceByKey(lambda x,y : x+y)

result.collect() 
# (1 job because of action collect)(2 satge because of wide dependency transformation
# reduce by key)(4 task because of min partition 2 and for each  satge there is 2 partition 
# so 2 stage 4 partition )


somato_rdd.map(lambda x : (x.split(",")[3])).countByValue() 
# (1 job because of action countByValue)(1 stage no wide transformation)(2 task as min partition 2 
# for 1 stage 2 task)


input("press enter to terminate")

spark.stop()