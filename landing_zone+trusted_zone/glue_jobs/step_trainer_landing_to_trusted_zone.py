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

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-nanodegree-s3-glue/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainer_landing_node1",
)

# Script generated for node Customer_curated
Customer_curated_node1686585281246 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data",
    table_name="customers_curated",
    transformation_ctx="Customer_curated_node1686585281246",
)

# Script generated for node Join
Join_node1686604982382 = Join.apply(
    frame1=Step_trainer_landing_node1,
    frame2=Customer_curated_node1686585281246,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1686604982382",
)

# Script generated for node Drop Fields
DropFields_node1686585351787 = DropFields.apply(
    frame=Join_node1686604982382,
    paths=[
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
        "registrationdate",
        "serialnumber",
        "birthday",
    ],
    transformation_ctx="DropFields_node1686585351787",
)

# Script generated for node Step_trainer_trusted
Step_trainer_trusted_node3 = glueContext.getSink(
    path="s3://udacity-nanodegree-s3-glue/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Step_trainer_trusted_node3",
)
Step_trainer_trusted_node3.setCatalogInfo(
    catalogDatabase="stedi_data", catalogTableName="step_trainer_trusted"
)
Step_trainer_trusted_node3.setFormat("json")
Step_trainer_trusted_node3.writeFrame(DropFields_node1686585351787)
job.commit()
