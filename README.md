# The system process data from Real estate website

## Description  
This idea of this project is building a system to process data from Real estate website . The project contains 3 parts : Crawl data from Real estate website through API , ETL pipeline to process raw date from MongoDB then store in MSSQL , create dashboards to visualize the real estate market in HCM city

Tech stack : Python , MongoDB , PySpark , Cassandra , SQL Server , PowerBI

## Tasks
### Crawl data : 
Contains raw data which is returned from website . Each day , the Crawl part will get the latest data from website and store in MongoDB

### Preprocess data :
Store processed data after going through ETL pipeline . This processed data will be used in further usage like : create dashboards

### Process data :
Steps of tramsforming raw data : 
1. Fill missing columns for raw data (Some data don't have enough features in given schema)
2. Convert Large number into Float 
3. Create Spark DataFrame
4. Drop rows which miss some important data such as information data
5. Join information data from the database to prevent the missing value in DataFrame
6. Fill null values based types of data
7. Update wrong list_time data and create new column `date` based on `list_time` column 


