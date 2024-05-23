from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("JobStageTaskExample").getOrCreate()

# Read data from a CSV file
employee_df = spark.read.format("csv") \
    .option("header", "true") \
    .load("C:\\Users\\TARUN\\Desktop\\Pyspark\\Employee1.csv")

# Repartition the DataFrame
employee_df = employee_df.repartition(2)

# Perform operations on the DataFrame
filtered_df = employee_df.filter(col('Salary') > 90000) \
    .select('Id', 'Name', 'Age', 'Salary', 'Loc') \
    .groupby('Loc').count()

# Action: Collect data (triggers the execution)
result = filtered_df.collect()

# Access the Spark application ID
application_id = spark.conf.get("spark.app.id")

# Access the Spark UI URL
spark_ui_url = spark.conf.get("spark.uiWebUrl")

# Get job information
jobs = spark.sparkContext.statusTracker().getJobIdsForGroup(application_id)

# Iterate through jobs to get stages and tasks
for job_id in jobs:
    job = spark.sparkContext.statusTracker().getJobInfo(job_id)
    print(f"Job {job.jobId}:")
    
    # Get stage information for the job
    stages = job.stageIds
    print(f"  Number of Stages: {len(stages)}")
    
    # Iterate through stages to get task information
    for stage_id in stages:
        stage = spark.sparkContext.statusTracker().getStageInfo(stage_id)
        print(f"    Stage {stage.stageId}:")
        print(f"      Number of Tasks: {stage.numTasks}")

# Stop the Spark session
spark.stop()
