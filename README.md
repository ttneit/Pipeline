# The system process data from Real estate website

## Description  
This idea of this project is building a system to process data from Real estate website . The project contains 3 parts : Crawl data from Real estate website through API , ETL pipeline to process raw date from MongoDB then store in MSSQL , create dashboards to visualize the real estate market in HCM city

Tech stack : Python , MongoDB , PySpark , Cassandra , SQL Server , PowerBI

## Architecture :  
![Architecture](resources\architecture.png)

## Tasks
### Crawl data : 
Crawl tasks : [Setup and Run file](./data_crawling/Setup.MD)

### Preprocess data :
Steps of preprocessing raw data before inserting into databses : 
1. Fill missing columns for raw data (Some data don't have enough features in given schema)
2. Convert Large number into Float 
3. Create Spark DataFrame
4. Drop rows which miss some important data such as information data
5. Join information data from the database to prevent the missing value in DataFrame
6. Fill null values based types of data
7. Update wrong list_time data and create new column `date` based on `list_time` column 

Scripts : [Scripts](./data_transformation/scripts/)

Database Schema : [Database Schema](./data_transformation/schema/)

### ETL pipeline :
Steps of ingesting preprocessed data: 
1. Retrieve the latest date of data in the source database
2. Preprocess data for each property type (house, land, office, apartment) to standardize columns and derive new attributes
3. Add property type information
4. Join the data with dimension tables to prepare it for loading into the fact table
5. Write the processed data to the fact table in the data warehouse

Scripts : [Scripts](./ETL_pipeline/scripts/)

Data Warehouse Schema : [Data Warehouse Schema](./ETL_pipeline/schema_dw/)


### Visualization  :
Visualization : [Visualization](visualization\real_estate_dashboards.pbix)
