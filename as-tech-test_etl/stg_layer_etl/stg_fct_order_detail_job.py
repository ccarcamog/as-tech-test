import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, regexp_replace, split, when, isnull
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "as-tech-test", table_name = "raw_dataset", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "as-tech-test", table_name = "raw_dataset", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("order_id", "int", "order_id", "int"), ("user_id", "int", "user_id", "int"), ("order_number", "int", "order_number", "int"), ("order_dow", "int", "order_dow", "int"), ("order_hour_of_day", "int", "order_hour_of_day", "int"), ("order_product", "string", "order_product", "string"), ("order_product_aisle", "string", "order_product_aisle", "string"), ("order_product_seq", "int", "order_product_seq", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("order_id", "int", "order_id", "int"), ("user_id", "int", "user_id", "int"), ("order_number", "int", "order_number", "int"), ("order_dow", "int", "order_dow", "int"), ("order_hour_of_day", "int", "order_hour_of_day", "int"), ("order_product", "string", "order_product", "string"), ("order_product_aisle", "string", "order_product_aisle", "string"), ("order_product_seq", "int", "order_product_seq", "int")], transformation_ctx = "applymapping1")
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
## @args: [connection_type = "s3", connection_options = {"path": "s3://as-tech-test/stg-layer/stg_fct_order_detail"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]

##Import Products DynamicFrame
datasourceProds = glueContext.create_dynamic_frame.from_catalog(database = "as-tech-test", table_name = "raw_products", transformation_ctx = "datasourceProds")
applymappingProds = ApplyMapping.apply(frame = datasourceProds, mappings = [("product_name", "string", "product_name", "string"), ("aisle", "string", "aisle", "string"), ("department", "string", "department", "string")], transformation_ctx = "applymappingProds")
resolvechoiceProds = ResolveChoice.apply(frame = applymappingProds, choice = "make_struct", transformation_ctx = "resolvechoiceProds")
dropnullfieldsProds = DropNullFields.apply(frame = resolvechoiceProds, transformation_ctx = "dropnullfieldsProds")

##Transform to DataFrames
datasetDf = dropnullfields3.toDF()
productsDf = dropnullfieldsProds.toDF()

##Join DataFrames
joinDf = datasetDf.join(productsDf, datasetDf.order_product == productsDf.product_name, how="left")

##Drop duplicated columns
dropCols = joinDf.drop("order_product")
dropCols = dropCols.drop("order_product_aisle")

#Back to DynamicFrame
cleaned_datasource = DynamicFrame.fromDF(dropCols, glueContext, "cleaned_datasource");

## Force one partition, so it can save only 1 file instead of 19
repartition = cleaned_datasource.repartition(1)


datasink4 = glueContext.write_dynamic_frame.from_options(frame = repartition, connection_type = "s3", connection_options = {"path": "s3://as-tech-test/stg-layer/stg_fct_order_detail"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()