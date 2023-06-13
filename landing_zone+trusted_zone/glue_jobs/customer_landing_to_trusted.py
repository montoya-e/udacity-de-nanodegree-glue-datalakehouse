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

# Script generated for node Landing_zone
Landing_zone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-nanodegree-s3-glue/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Landing_zone_node1",
)

# Script generated for node customers_research_shared
customers_research_shared_node1686581690781 = Filter.apply(
    frame=Landing_zone_node1,
    f=lambda row: (row["shareWithResearchAsOfDate"] > 0),
    transformation_ctx="customers_research_shared_node1686581690781",
)

# Script generated for node Trusted_zone
Trusted_zone_node3 = glueContext.getSink(
    path="s3://udacity-nanodegree-s3-glue/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Trusted_zone_node3",
)
Trusted_zone_node3.setCatalogInfo(
    catalogDatabase="stedi_data", catalogTableName="customer_trusted"
)
Trusted_zone_node3.setFormat("json")
Trusted_zone_node3.writeFrame(customers_research_shared_node1686581690781)
job.commit()
