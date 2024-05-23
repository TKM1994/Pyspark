from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder.appName("job_stage_task").getOrCreate()


employee_df = spark.read.format("csv")\
    .option("header", "true")\
    .load("C:\\Users\\TARUN\\Desktop\\Pyspark\\Employee1.csv")

employee_df = employee_df.repartition(2)

employee_df = employee_df.filter(col('Salary') > 90000)\
    .select('Id', 'Name', 'Age', 'Salary', 'Loc')\
    .groupby('Loc').count()


employee_df.collect()

employee_df.show()

input("press enter to terminate")
