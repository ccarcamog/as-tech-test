import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp
from awsglue.dynamicframe import DynamicFrame

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "as-tech-test", table_name = "stg_dim_product", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "as-tech-test", table_name = "stg_dim_product", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("product_name", "string", "dim_product_name", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("product_name", "string", "product_name", "string"),("aisle", "string", "aisle", "string"),("department", "string", "department", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["dim_product_key", "dim_aisle_key", "dim_product_insert_dt", "dim_department_key", "dim_product_name"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
##selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["dim_product_key", "dim_aisle_key", "dim_product_insert_dt", "dim_department_key", "dim_product_name"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "as-redshift-dw", table_name = "as_tech_test_public_dim_product", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
##resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "as-redshift-dw", table_name = "as_tech_test_public_dim_product", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice4")

##Import dim_aisle
datasourceAisle = glueContext.create_dynamic_frame.from_catalog(database = "as-redshift-dw", table_name = "as_tech_test_public_dim_aisle", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasourceAisle")
applymappingAisle = ApplyMapping.apply(frame = datasourceAisle, mappings = [("dim_aisle_key", "int", "dim_aisle_key", "int"), ("dim_aisle_name", "string", "dim_aisle_name", "string"), ("dim_aisle_insert_dt", "timestamp", "dim_aisle_insert_dt", "timestamp")], transformation_ctx = "applymappingAisle")
resolvechoiceAisle = ResolveChoice.apply(frame = applymappingAisle, choice = "make_struct", transformation_ctx = "resolvechoiceAisle")

##Import dim_department
datasourceDep = glueContext.create_dynamic_frame.from_catalog(database = "as-redshift-dw", table_name = "as_tech_test_public_dim_department", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasourceDep")
applymappingDep = ApplyMapping.apply(frame = datasourceDep, mappings = [("dim_department_key", "int", "dim_department_key", "int"), ("dim_department_name", "string", "dim_department_name", "string"), ("dim_department_insert_dt", "timestamp", "dim_department_insert_dt", "timestamp")], transformation_ctx = "applymappingDep")
resolvechoiceDep = ResolveChoice.apply(frame = applymappingDep, choice = "make_struct", transformation_ctx = "resolvechoiceDep")

##Transform to DataFrames
productsDf = resolvechoice4.toDF()
aislesDf = resolvechoiceAisle.toDF()
depDf = resolvechoiceDep.toDF()

##Join DataFrames
join1Df = productsDf.join(aislesDf, productsDf.aisle == aislesDf.dim_aisle_name)
joinDf = join1Df.join(depDf, join1Df.department == depDf.dim_department_name)

##Drop duplicated columns
##dropCols = joinDf.drop("aisle")
##dropCols = dropCols.drop("department")
##dropCols = dropCols.drop("dim_aisle_insert_dt")
##dropCols = dropCols.drop("dim_aisle_name")

##Rename columns
renamedCol = joinDf.select(col("product_name").alias("dim_product_name"), col("dim_aisle_key"), col("dim_department_key"))

##Add Timestamp
timestampedDf = renamedCol.withColumn("dim_product_insert_dt", current_timestamp())

##Back to DynamicFrame
cleaned_datasource = DynamicFrame.fromDF(timestampedDf, glueContext, "cleaned_datasource");

## @type: DataSink
## @args: [database = "as-redshift-dw", table_name = "as_tech_test_public_dim_product", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = cleaned_datasource, database = "as-redshift-dw", table_name = "as_tech_test_public_dim_product", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()