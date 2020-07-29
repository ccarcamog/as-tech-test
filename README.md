# Applaudo Studios Technical Test - ETL Developer
Hi everyone! I this file, I will try to explain the entire data analysis process to give a functional solution for the assessment case. Some resources will be sent via email. For any comments or questions, you can contact me to email ccarcamog92@gmail.com.

## Solution Analysis
The requirement is based on a supermarket performance analysis. There are different data sources, achieving one goal: the unification of data for further analysis and visualizations. Based on this, the best approach is to implement a Big Data solution, to extract the data from multiple sources, transform it via ETL processes and store it in a one and only data store (Data Warehouse).

As the solution will need a Big Data approach, there are several ways to achive it, using a lot of technologies. To create a escalar, well-performed and easy to manage solution, the best approach it takes the solution to the cloud. For this assessment, I will develop the entire solution using AWS tools, except for data visualization, that I will be using Power BI.

## Data Architecture
Now that I have defined AWS as my cloud provider, the entire solution will take place using the following architecture:
![Data_Architecture](/readme_img/data-arq.png)

For more information about the technologies to be used, please check the following information:
- [Amazon S3](https://aws.amazon.com/s3/)
- [AWS Glue](https://aws.amazon.com/glue/)
- [Amazon Redshift](https://aws.amazon.com/es/redshift/)
- [Power BI](https://powerbi.microsoft.com/es-es/what-is-power-bi/)

### Data Layers
- **Data sources:** Will be the flat files provided for the solutions of this assessment, without any transformation or change. The files will be allocated in S3 for this test, but they can be allocated in other sources.
- **Raw Layer:** Will allocate the files with cleaning and cleansing transformations. Any issue with data will be solved in this stage. AWS Glue will be in charge to extract data from Data sources layer, transform the data and store it again in S3. The data in this stage has no analytic purposes. All the data will be stored in Parquet files.
- **Stage Layer:** Will be the previous layer for presentation porpuses. in this stage, the data will be allocated according to the dimensional model. The data will be catalogued by dimension or fact table. AWS Glue, will select the data for each dimension or fact table from the data sources. This Layer will be stored in S3. All the data will be stored in Parquet files.
- **Dimensional Layer:** Also named as Presentation Layer. The dimensional model will be allocated here. All the dimension and fact fill be related via surrogate keys. Amazon Redshift will be used for this purposes. Power BI will extract the data directly from Redshift for Data Visualizations.

If you want to lear a lit bit of whay using Parquet files in non-analytics stage is a great idea, I recommend to read [this article.](https://acadgild.com/blog/parquet-file-format-hadoop)

## Dimensional Model
My proposal as Dimensional Model, for this solution is the following:
![DimModel](/readme_img/dm.png)

I propose a Kimball Star Model, with a Fact Table storing all transactions at order detail level. As you can see, there is a relation between each dimension with a key in the Fact Table. Except for dim_aisle and dim_department, besides having a relationship with Fact Table table, they have relation with dim_product. In a first moment, I decided to use a dimension to store 3 units in one table, but thinking of having independent catalogues for aisle and department can help to give an easier maintenance. Both dimensions will work as [outrigger dimensions.](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/outrigger-dimension/) 

I will describe each table. The data dictionary for each table will be ready in the database, so I won't give more details in this file.

### FCT_ORDER_DETAIL
- **Description:** The central table in the star. This fact table was created to represent a transaction generated by the store. Will have communication with all the dimensions to filter or group the data. Will store the entire detail of products that were sold. 
- **Granularity:** One line per product sell in order.

### DIM_PRODUCT
- **Description:** Represents a single product from the store. Basically, is the transformation of the products.json file. Will store the product attributes to the Fact Table. will be the master table from dim_aisle and dim_department.
- **Granularity:** One line per single product.

### DIM_AISLE
- **Description:** Represents a catalog of different aisle from the products.
- **Granularity:** One line per single aisle.

### DIM_AISLE
- **Description:** Represents a catalog of different department from the products.
- **Granularity:** One line per single department.

### DIM_USER
- **Description:** Represents the user that made the purchase. The dimension right now, just is the User ID, as we don't have more user's data in the data sources, but the model will be open to add any attribute regarding to Users.
- **Granularity:** One line per single user.

### DIM_DAY_OF_WEEK
- **Description:** Represents the day in which a order was generated.
- **Granularity:** One line per single day of week.

### DIM_HOUR_OF_DAY
- **Description:** Represents the hour in which a order was generated.
- **Granularity:** One line per single hour of day.

## Exploratory Data Analysis
Now the solution was designed, it's time to start checking the data. The Exploratory Data Analysis, helped me to find data issues in the data sources. All the issues found in this analysis were successfully boarded in the Raw Layer of the solution.

I checked both data source files, trying to find any incosinstency. 

To check the complete Exploratory Data Analysis, please check this [Jupyter Notebook.](https://github.com/ccarcamog/as-tech-test/blob/master/AS%20Tech%20Test%20-%20Expolatory%20Data%20Analysis.ipynb)

## ETL Development

The entire ETL was developed on AWS Glue. Glue was present in all the extractions, from datasources to Redshift. Glue is a great tool if you don't have high-change data sources, and if your entire solution is developed on AWS. 

There are 3 main conceps on AWS Glue that I mastered with this assessment. All of them are parts of the ETL, and help with the extraction of multiple datatypes and files formats, transformations and load on Redshift. I will give more detail of these elements here:

### Crawlers
Crawlers look for the data structure of sources and destinations in an automated way. A crawler creates the data catalogue from a specific source/destination structure whatever the type it is (database tables, flat files, CSV files, JSON files, PARQUET files, etc.). You can check more information about Glue crawlers [here.](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html)

For this project, I have created a crawler for each data source file, for each target table in the Raw Layer, for each target table in the Stage Layer, and for each target table in the Redshift Data Warehouse:
![Crawlers](/readme_img/crawlers.png)

As you can notice, I haven't created a crawler for the datasource.csv file. This is because the crawler thrown an error when I ran it for the encoding of the file. To solve this issue I created manually the Table, and then I changed the encoding.

### Tables
Tables are objects generated by the crawlers. Contains the data catalogue for the data structure you will need to use as a source or target in an ETL process. You need to have a Glue table for each source or target that will be used in the ETL, because these structures store the data dictionary, datatypes and paths for the ETL:
![Tables](/readme_img/tables.png)

As you can see, I don't have any Glue table for Stage Layer for dim_day_of_week and dim_hour_of_day dimensions. This is because I didn´t create an ETL for them. I created them manually and filled them with their data.

### Jobs
Finally, it's time to create the processes to move and transform the data between layers. Jobs are PySpark procedures created to run a specific task. A Job start with the extraction of data from the source table and finish with the load of the data in the target table. To make transformations between these steps, you can use PySpark Dataframes. I have created a job for each dim from my dimensional model in the different layers, and for each data source:
![Jobs](/readme_img/jobs.png)

As you can see, there is a parameter named "Job bookmark". If this parameter is Enabled, Glue saves states of loads to remember de data loaded. So, with this, we can set an incremental load, to avoid duplications of data in the target, en just insert new data that wasn't present in the last load.

The definition of all the jobs I have created are available in [this repository.](https://github.com/ccarcamog/as-tech-test/tree/master/as-tech-test_etl) All processes were developed on Python, and all of them have more transformation about Glue's default ones.

If you want more explanation about Glue processes, don't hesitate to contact me.

## Access to Data
After all processes ran successfully, you may want to see data. you can see and check data from all the layers using the credentials shared on email:
- **Amazon Redshift Database for Dim Layer:** I have shared the connection string and parameters on email.
- **Amazon S3 for Raw and Stg Layer:** I have shared the keys to connect to bucket. If you want to see full structure of folders, you can access to my S3 bucket using [S3 Browser](https://s3browser.com/). You can check the data in the Parquet files using [ParquetViewer](https://github.com/mukunku/ParquetViewer).

## Data Visualization
Finally, after the entire Data Wrangling process is over, it's time create visuals. I have created a Power BI report to represent the data extracted and loaded. You can check the report [here.](https://app.powerbi.com/view?r=eyJrIjoiODdiODJiYzYtODU0OS00MGY5LTg3OTgtNDU5ODI5Y2QxNWIyIiwidCI6ImM2N2Y3Mjg3LWRkMDAtNDZjMC1iOTUzLTA2NWZiYjIyY2ExNiJ9&pageName=ReportSection) The source file for this visualization is stored in the S3 bucket.

After checking the data, I can stablish some conclusion about the data:
1. Sunday is the day when more orders are processed. 21% of orders are processed on sundays. This could mean, that clients prefer to go to the store on weekends to have supplements for all the week.
1. The rush hours in the store are between 10am and 4pm. These can lead to thinks is could be a good idea to close the store between 10pm and 6am.
1. Clients love fruits and vegetable. Those are the most visited aisles in the supermarket.


