from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("job_stage_task").getOrCreate()

orders_rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/tkm/data/orders.csv")

header = orders_rdd.first()  #fist job

data_rdd = orders_rdd.filter(lambda x : x != header)

status_rdd = data_rdd.map(lambda x : (x.split(",")[4] , 1))

result = status_rdd.reduceByKey(lambda x,y : x+y) #2nd stage in job 2

result.collect() #2nd job

input("press enter to terminate")

spark.stop()