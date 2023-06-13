import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometer_trusted_node1",
)

# Script generated for node Step_trainer_trusted
Step_trainer_trusted_node1686617703514 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data",
    table_name="step_trainer_trusted",
    transformation_ctx="Step_trainer_trusted_node1686617703514",
)

# Script generated for node Join
Join_node1686617742511 = Join.apply(
    frame1=Step_trainer_trusted_node1686617703514,
    frame2=Accelerometer_trusted_node1,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1686617742511",
)

# Script generated for node Drop Fields
DropFields_node1686617790362 = DropFields.apply(
    frame=Join_node1686617742511,
    paths=["user"],
    transformation_ctx="DropFields_node1686617790362",
)

# Script generated for node machine_learning
machine_learning_node1686618055351 = glueContext.getSink(
    path="s3://udacity-nanodegree-s3-glue/machine_learning/landing/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_node1686618055351",
)
machine_learning_node1686618055351.setCatalogInfo(
    catalogDatabase="stedi_data", catalogTableName="machine_learning_curated"
)
machine_learning_node1686618055351.setFormat("json")
machine_learning_node1686618055351.writeFrame(DropFields_node1686617790362)
job.commit()
