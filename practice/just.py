from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark =  SparkSession.builder.appName("just").getOrCreate()

import pandas as pd

schema = StructType([ 
    StructField("firstname", StringType(), True), 
    StructField("middlename", StringType(), True), 
    StructField("lastname", StringType(), True), 
    StructField("id", IntegerType(), True), 
    StructField("gender", StringType(), True), 
    ])

person_list = [
    ("Berry", "", "Allen", 1, "M"),
    ("Oliver", "Queen", "", 2, "M"),
    ("Robert", "", "Williams", 3, "M"),
    ("Tony", "", "Stark", 4, "F"),
    ("Rajiv", "Mary", "Kumar", 5, "F")
]

print(person_list)

dataframe = pd.DataFrame(person_list)

print(dataframe)

df = spark.createDataFrame(data = person_list,schema = schema)

df.printSchema()

df.show()


#input("enter")