import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "as-tech-test", table_name = "products_json_files", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "as-tech-test", table_name = "products_json_files", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("product_name", "string", "product_name", "string"), ("aisle", "string", "aisle", "string"), ("department", "string", "department", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("product_name", "string", "product_name", "string"), ("aisle", "string", "aisle", "string"), ("department", "string", "department", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://as-tech-test/stg-layer/stg_dim_product"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]

##Drop duplicates
dedupedDf = dropnullfields3.toDF().dropDuplicates()

#Back to DynamicFrame
cleaned_datasource = DynamicFrame.fromDF(dedupedDf, glueContext, "cleaned_datasource");

## Force one partition, so it can save only 1 file instead of 19
repartition = cleaned_datasource.repartition(1)

datasink4 = glueContext.write_dynamic_frame.from_options(frame = repartition, connection_type = "s3", connection_options = {"path": "s3://as-tech-test/stg-layer/stg_dim_product"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()