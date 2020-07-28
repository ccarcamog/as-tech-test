import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, concat, lit
from awsglue.dynamicframe import DynamicFrame

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "as-tech-test", table_name = "stg_fct_order_detail", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "as-tech-test", table_name = "stg_fct_order_detail", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("order_id", "int", "fct_order_detail_order_num", "int"), ("user_id", "int", "fct_order_detail_order_seq", "int"), ("order_number", "int", "fct_order_detail_user_key", "int"), ("order_dow", "int", "fct_order_detail_order_id", "int"), ("order_hour_of_day", "int", "fct_order_detail_hour_of_day_key", "int"), ("order_product", "string", "fct_order_detail_insert_dt", "timestamp"), ("order_product_aisle", "string", "fct_order_detail_order_integration_id", "string"), ("order_product_seq", "int", "fct_order_detail_product_key", "int"), ("product_name", "string", "fct_order_detail_aisle_key", "int"), ("aisle", "string", "fct_order_detail_prods_per_order", "int"), ("department", "string", "fct_order_detail_department_key", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("order_id", "int", "order_id", "int"), ("user_id", "int", "user_id", "int"), ("order_number", "int", "order_number", "int"), ("order_dow", "int", "order_dow", "int"), ("order_hour_of_day", "int", "order_hour_of_day", "int"), ("order_product_seq", "int", "order_product_seq", "int"), ("product_name", "string", "product_name", "string"), ("aisle", "string", "aisle", "string"), ("department", "string", "department", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["fct_order_detail_prods_per_order", "fct_order_detail_department_key", "fct_order_detail_order_num", "fct_order_detail_product_key", "fct_order_detail_order_seq", "fct_order_detail_order_integration_id", "fct_order_detail_insert_dt", "fct_order_detail_aisle_key", "fct_order_detail_user_key", "fct_order_detail_day_of_week_key", "fct_order_detail_hour_of_day_key", "fct_order_detail_order_id"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
#selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["fct_order_detail_prods_per_order", "fct_order_detail_department_key", "fct_order_detail_order_num", "fct_order_detail_product_key", "fct_order_detail_order_seq", "fct_order_detail_order_integration_id", "fct_order_detail_insert_dt", "fct_order_detail_aisle_key", "fct_order_detail_user_key", "fct_order_detail_day_of_week_key", "fct_order_detail_hour_of_day_key", "fct_order_detail_order_id"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "as-redshift-dw", table_name = "as_tech_test_public_fct_order_detail", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
#resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "as-redshift-dw", table_name = "as_tech_test_public_fct_order_detail", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice4")

##Import dim_product
datasourceProd = glueContext.create_dynamic_frame.from_catalog(database = "as-redshift-dw", table_name = "as_tech_test_public_dim_product", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasourceProd")
applymappingProd = ApplyMapping.apply(frame = datasourceProd, mappings = [("dim_product_key", "int", "dim_product_key", "int"), ("dim_aisle_key", "int", "dim_aisle_key", "int"), ("dim_product_insert_dt", "timestamp", "dim_product_insert_dt", "timestamp"), ("dim_department_key", "int", "dim_department_key", "int"), ("dim_product_name", "string", "dim_product_name", "string")], transformation_ctx = "applymappingProd")
resolvechoiceProd = ResolveChoice.apply(frame = applymappingProd, choice = "make_struct", transformation_ctx = "resolvechoiceProd")

##Import dim_user
datasourceUser = glueContext.create_dynamic_frame.from_catalog(database = "as-redshift-dw", table_name = "as_tech_test_public_dim_user", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasourceUser")
applymappingUser = ApplyMapping.apply(frame = datasourceUser, mappings = [("dim_user_key", "int", "dim_user_key", "int"), ("dim_user_insert_dt", "timestamp", "dim_user_insert_dt", "timestamp"), ("dim_user_id", "int", "dim_user_id", "int")], transformation_ctx = "applymappingUser")
resolvechoiceUser = ResolveChoice.apply(frame = applymappingUser, choice = "make_struct", transformation_ctx = "resolvechoiceUser")

##Import dim_day_of_week
datasourceDow = glueContext.create_dynamic_frame.from_catalog(database = "as-redshift-dw", table_name = "as_tech_test_public_dim_day_of_week", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasourceDow")
applymappingDow = ApplyMapping.apply(frame = datasourceDow, mappings = [("dim_day_of_week_key", "int", "dim_day_of_week_key", "int"), ("dim_day_of_week_insert_dt", "timestamp", "dim_day_of_week_insert_dt", "timestamp"), ("dim_day_of_week_id", "int", "dim_day_of_week_id", "int"), ("dim_day_if_week_name", "string", "dim_day_if_week_name", "string")], transformation_ctx = "applymappingDow")
resolvechoiceDow = ResolveChoice.apply(frame = applymappingDow, choice = "make_struct", transformation_ctx = "resolvechoiceDow")

##Import dim_hour_of_day
datasourceHod = glueContext.create_dynamic_frame.from_catalog(database = "as-redshift-dw", table_name = "as_tech_test_public_dim_hour_of_day", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasourceHod")
applymappingHod = ApplyMapping.apply(frame = datasourceHod, mappings = [("dim_hour_of_day_key", "int", "dim_hour_of_day_key", "int"), ("dim_hour_of_day_insert_dt", "timestamp", "dim_hour_of_day_insert_dt", "timestamp"), ("dim_hour_of_day_label", "int", "dim_hour_of_day_label", "int")], transformation_ctx = "applymappingHod")
resolvechoiceHod = ResolveChoice.apply(frame = applymappingHod, choice = "make_struct", transformation_ctx = "resolvechoiceHod")

##Transform to DataFrames
orderDetailDf = resolvechoice4.toDF()
productsDf = resolvechoiceProd.toDF()
usersDf = resolvechoiceUser.toDF()
dowDf = resolvechoiceDow.toDF()
hodDf = resolvechoiceHod.toDF()

##Join DataFrames
join1Df = orderDetailDf.join(productsDf, orderDetailDf.product_name == productsDf.dim_product_name, how="left")
join2Df = join1Df.join(usersDf, join1Df.user_id == usersDf.dim_user_id, how="left")
join3Df = join2Df.join(dowDf, join2Df.order_dow == dowDf.dim_day_of_week_id, how="left")
join4Df = join3Df.join(hodDf, join3Df.order_hour_of_day == hodDf.dim_hour_of_day_label, how="left")

##Select columns
selCol = join4Df.select(col("dim_user_key").alias("fct_order_detail_user_key"), col("dim_day_of_week_key").alias("fct_order_detail_day_of_week_key"), col("dim_hour_of_day_key").alias("fct_order_detail_hour_of_day_key"), col("dim_product_key").alias("fct_order_detail_product_key"), col("dim_aisle_key").alias("fct_order_detail_aisle_key"), col("dim_department_key").alias("fct_order_detail_department_key"), col("order_id").alias("fct_order_detail_order_id"), col("order_number").alias("fct_order_detail_order_num"), col("order_product_seq").alias("fct_order_detail_order_seq"), concat(col("order_id"),lit("-"),col("order_product_seq")).alias("fct_order_detail_order_integration_id"))

##Add Timestamp
timestampedDf = selCol.withColumn("fct_order_detail_insert_dt", current_timestamp())

##Add default metric
metricDf = timestampedDf.withColumn("fct_order_detail_prods_per_order", lit(1))

##Add Intregration Id
##intIdDf = metricDf.withColumn("fct_order_detail_order_integration_id", concat(col("fct_order_detail_order_id"),lit("-"),col("fct_order_detail_order_seq")))

##Back to DynamicFrame
cleaned_datasource = DynamicFrame.fromDF(metricDf, glueContext, "cleaned_datasource");

## @type: DataSink
## @args: [database = "as-redshift-dw", table_name = "as_tech_test_public_fct_order_detail", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = cleaned_datasource, database = "as-redshift-dw", table_name = "as_tech_test_public_fct_order_detail", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()