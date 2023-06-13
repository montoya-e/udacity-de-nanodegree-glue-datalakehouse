import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer_landing_zone
Accelerometer_landing_zone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-nanodegree-s3-glue/accelerometer/landing"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_landing_zone_node1",
)

# Script generated for node Customer_landing_zone
Customer_landing_zone_node1686581982448 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data",
    table_name="customer_landing",
    transformation_ctx="Customer_landing_zone_node1686581982448",
)

# Script generated for node Join
Join_node1686582023089 = Join.apply(
    frame1=Customer_landing_zone_node1686581982448,
    frame2=Accelerometer_landing_zone_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1686582023089",
)

# Script generated for node customers_research_shared
customers_research_shared_node1686581690781 = Filter.apply(
    frame=Join_node1686582023089,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="customers_research_shared_node1686581690781",
)

# Script generated for node Drop Fields
DropFields_node1686582358726 = DropFields.apply(
    frame=customers_research_shared_node1686581690781,
    paths=["phone", "email"],
    transformation_ctx="DropFields_node1686582358726",
)

# Script generated for node Trusted_zone
Trusted_zone_node3 = glueContext.getSink(
    path="s3://udacity-nanodegree-s3-glue/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Trusted_zone_node3",
)
Trusted_zone_node3.setCatalogInfo(
    catalogDatabase="stedi_data", catalogTableName="accelerometer_trusted"
)
Trusted_zone_node3.setFormat("json")
Trusted_zone_node3.writeFrame(DropFields_node1686582358726)
job.commit()
