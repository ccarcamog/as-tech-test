import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp
from awsglue.dynamicframe import DynamicFrame

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "as-tech-test", table_name = "stg_dim_department", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "as-tech-test", table_name = "stg_dim_department", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("department", "string", "dim_department_name", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("department", "string", "dim_department_name", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["dim_department_insert_dt", "dim_department_name", "dim_department_key"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["dim_department_insert_dt", "dim_department_name", "dim_department_key"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "as-redshift-dw", table_name = "as_tech_test_public_dim_department", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "as-redshift-dw", table_name = "as_tech_test_public_dim_department", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

##get Insert Date
timestampedDf = resolvechoice4.toDF().withColumn("dim_department_insert_dt", current_timestamp())

#Back to DynamicFrame
cleaned_datasource = DynamicFrame.fromDF(timestampedDf, glueContext, "cleaned_datasource");

## @type: DataSink
## @args: [database = "as-redshift-dw", table_name = "as_tech_test_public_dim_department", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = cleaned_datasource, database = "as-redshift-dw", table_name = "as_tech_test_public_dim_department", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()