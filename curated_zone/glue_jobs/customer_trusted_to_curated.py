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

# Script generated for node Customer_trusted
Customer_trusted_node1686585036821 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data",
    table_name="customer_trusted",
    transformation_ctx="Customer_trusted_node1686585036821",
)

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_landing_node1",
)

# Script generated for node Join
Join_node1686585072818 = Join.apply(
    frame1=Customer_trusted_node1686585036821,
    frame2=Accelerometer_landing_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1686585072818",
)

# Script generated for node Drop Fields
DropFields_node1686585101084 = DropFields.apply(
    frame=Join_node1686585072818,
    paths=["x", "y", "z", "user", "timestamp", "email", "phone", "customername"],
    transformation_ctx="DropFields_node1686585101084",
)

# Script generated for node Customer_curated
Customer_curated_node3 = glueContext.getSink(
    path="s3://udacity-nanodegree-s3-glue/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Customer_curated_node3",
)
Customer_curated_node3.setCatalogInfo(
    catalogDatabase="stedi_data", catalogTableName="customer_curated"
)
Customer_curated_node3.setFormat("json")
Customer_curated_node3.writeFrame(DropFields_node1686585101084)
job.commit()
