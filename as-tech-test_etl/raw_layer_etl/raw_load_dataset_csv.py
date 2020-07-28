import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "as-tech-test", table_name = "dc-dataset", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "as-tech-test", table_name = "dc-dataset", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("order_id", "string", "order_id", "string"), ("user_id", "string", "user_id", "string"), ("order_number", "string", "order_number", "string"), ("order_dow", "string", "order_dow", "string"), ("order_hour_of_day", "string", "order_hour_of_day", "string"), ("order_detail", "string", "order_detail", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("order_id", "string", "order_id", "string"), ("user_id", "string", "user_id", "string"), ("order_number", "string", "order_number", "string"), ("order_dow", "string", "order_dow", "string"), ("order_hour_of_day", "string", "order_hour_of_day", "string"), ("order_detail", "string", "order_detail", "string")], transformation_ctx = "applymapping1")
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

##Split
explode_df = dropnullfields3.toDF().withColumn("order_detail",explode(split(col("order_detail"),"~")))

##Split Columns
split_df = explode_df.withColumn("order_product", split(col("order_detail"), "\|").getItem(0)).withColumn("order_product_aisle", split(col("order_detail"), "\|").getItem(1)).withColumn("order_product_seq", split(col("order_detail"), "\|").getItem(2))

##Remove order_detail column (as it is splitted)
noOrderDetail = split_df.drop("order_detail")

##Remove headers from dataFrame
headers = noOrderDetail.filter(noOrderDetail["order_product"] == "ORDER_DETAIL")
noHeadersDf = noOrderDetail.subtract(headers)

##Change string values from ORDER_DOW
noStringDOW = noHeadersDf.withColumn("order_dow", when(noHeadersDf["order_dow"] == "Tuesday", "2").otherwise(noHeadersDf["order_dow"]))
noStringDOW = noStringDOW.withColumn("order_dow", when(noStringDOW["order_dow"] == "Friday", "5").otherwise(noStringDOW["order_dow"]))

##Change 24 value to 0 from ODER_HOUR_OF_DAY
no24HOD = noStringDOW.withColumn("order_hour_of_day", when(noStringDOW["order_hour_of_day"] == "24", "0").otherwise(noStringDOW["order_hour_of_day"]))

##Change product_names with issues
prodNamesFix = no24HOD.withColumn("order_product", when(no24HOD["order_product"] == "Pull?Ups Learning Designs Boys Size 2T-3T Training Pants Disney Pixar", "Pull‑Ups Learning Designs Boys Size 2T-3T Training Pants Disney Pixar").otherwise(no24HOD["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "Hershey's Chocolate Cr?eme Pie Singles", "Hershey's Chocolate Creme Pie Singles").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "Bathroom Tissue, 1?Ply, 1000 Sheet Rolls", "Bathroom Tissue, 1‑Ply, 1000 Sheet Rolls").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "A.R. Milk Based Infant Formula for Spit?Up Powder", "A.R. Milk Based Infant Formula for Spit‑Up Powder").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "Zero Lemon?Lime Soda", "Zero Lemon‑Lime Soda").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "Soft?Ripened Brie Cheese", "Soft‑Ripened Brie Cheese").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "Harvest Breakfast Fruit?Filled Strawberry Squares", "Harvest Breakfast Fruit‑Filled Strawberry Squares").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "Natur?'s Calorie-Free Sweetener", "Nature's Calorie-Free Sweetener").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "360? Floss-tip Bristles Powered Toothbrush Soft", "360˚ Floss-tip Bristles Powered Toothbrush Soft").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "Diana?s Bananas Milk Chocolate with Peanuts - Banana Babies ? 5 CT", "Diana�s Bananas Milk Chocolate with Peanuts - Banana Babies � 5 CT").otherwise(prodNamesFix["order_product"]))
prodNamesFix = prodNamesFix.withColumn("order_product", when(prodNamesFix["order_product"] == "Pull-Ups Night?Time Training Pants 2T?3T", "Pull-Ups Night‑Time Training Pants 2T‑3T").otherwise(prodNamesFix["order_product"]))


##Change datatypes
datatypesDf = prodNamesFix.withColumn("order_id", prodNamesFix["order_id"].cast("int"))
datatypesDf = datatypesDf.withColumn("user_id", datatypesDf["user_id"].cast("int"))
datatypesDf = datatypesDf.withColumn("order_number", datatypesDf["order_number"].cast("int"))
datatypesDf = datatypesDf.withColumn("order_dow", datatypesDf["order_dow"].cast("int"))
datatypesDf = datatypesDf.withColumn("order_hour_of_day", datatypesDf["order_hour_of_day"].cast("int"))
datatypesDf = datatypesDf.withColumn("order_hour_of_day", datatypesDf["order_hour_of_day"].cast("int"))
datatypesDf = datatypesDf.withColumn("order_product_seq", datatypesDf["order_product_seq"].cast("int"))

#Back to DynamicFrame
cleaned_datasource = DynamicFrame.fromDF(datatypesDf, glueContext, "cleaned_datasource");

## Force one partition, so it can save only 1 file instead of 19
repartition = cleaned_datasource.repartition(1)

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://as-tech-test/raw-layer"}, format = "parquet", transformation_ctx = "datasink2"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = repartition, connection_type = "s3", connection_options = {"path": "s3://as-tech-test/raw-layer/dataset"}, format = "parquet", transformation_ctx = "datasink2")
job.commit()