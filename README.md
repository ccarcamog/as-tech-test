# Applaudo Studios Technical Test - ETL Developer
Hi everyone! I this file, I will try to explaine the entire data analysis process to give a functional solution for the assessment case. Some resources will be sent via email. For any comments or questions you can contact me to email ccarcamog92@gmail.com.

## Solution Analysis
The requirement is based on a supermarket performance analysis. There are different data sources, achiving one goal: the unification of data for further analysis and visualizations. Based on this, the best approach is to implement a Big Data solution, to extract the data from multiple sources, transforme it via ETL processes and store it in a one and only data store (Data Warehouse).

As the solution will need a big Data approch, there are several ways to achive it, using a lot of tecnologies. To create a escalar, well-performed and easy to manage solution, the best approuch it take the solution to the cloud. For this assessment, I will develop the entire solution using AWS tools, except for data visualization, that I will be using Power BI.

## Data Architecture
Now that I have defined AWS as my cloud provider, the entire solution will take place using the following architecture:
![Data_Architecture](/readme_img/data-arq.png)

For more information about the tecnologies to be used, please check the following information:
- [Amazon S3](https://aws.amazon.com/s3/)
- [AWS Glue](https://aws.amazon.com/glue/)
- [Amazon Redshift](https://aws.amazon.com/es/redshift/)
- [Power BI](https://powerbi.microsoft.com/es-es/what-is-power-bi/)

### Data Layers
- **Data sources:** Will be the flat files provided for the solutions of this assesment, without any transformation or change. The files will be allocated in S3 for this test, but they can be allocated in other source.
- **Raw Layer:** Will allocate the files with cleaning and cleasing transformations. Any issue with data will be solved in this stage. AWS Glue will be in charge to extract data from Data surves layer, transform the data and store it again in S3. The data in this stage, has no analytic porpuses.
- **Stage Layer:** Will be the previous layer for presentation porpuses. in this stage, the data will be allocated according to the dimensional model. The data will be cataloged by dimension or fact table. AWS Glue, will select the data for each dimension or fact table from the data sources. This Layer will be stored in S3.
- **Dim Layer:** Also named as Presentation Layer. The dimensional model will be allocated here. All the dimension and fact fill be related via surrogate keys. Amazon Redshift will be used for this porpouses. Power BI will extract the data directrly from Redshilft for Data Visualizations.
