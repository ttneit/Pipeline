import pymongo
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from uuid import * 
from pyspark.sql.window import Window as W
import datetime
from pyspark.sql.types import NumericType

def preprocessing_col(element) : 
    full_col = ['ad_id', 'list_id', 'list_time', 'orig_list_time', 'date', 'account_id', 'account_oid', 'account_name', 'state', 'subject', 'body', 'category', 'category_name', 'area', 'area_name', 'region', 'region_name', 'company_ad', 'type', 'price', 'price_string', 'image', 'webp_image', 'number_of_images', 'avatar', 'property_legal_document', 'size', 'region_v2', 'area_v2', 'ward', 'ward_name', 'direction', 'price_million_per_m2', 'land_type', 'width', 'length', 'contain_videos', 'location', 'longitude', 'latitude', 'phone_hidden', 'owner', 'zero_deposit', 'ad_labels', 'special_display', 'has_video', 'shop_alias']
    
    for col in full_col : 
        if col not in element.keys() : 
            element[col] = None
    
    numeric_col = ['list_time', 'orig_list_time', 'price', 'size', 'price_million_per_m2', 'width', 'length', 'longitude', 'latitude']

    for col in numeric_col : 
        element[col] = float(element[col]) if element[col] is not None else None
    return element



def create_spark(docs) : 
    schema = StructType([
        StructField("ad_id", IntegerType(), True),
        StructField("list_id", IntegerType(), True),
        StructField("list_time", FloatType(), True),
        StructField("orig_list_time", FloatType(), True),
        StructField("date", StringType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("account_oid", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("body", StringType(), True),
        StructField("category", IntegerType(), True),
        StructField("category_name", StringType(), True),
        StructField("area", IntegerType(), True),
        StructField("area_name", StringType(), True),
        StructField("region", IntegerType(), True),
        StructField("region_name", StringType(), True),
        StructField("company_ad", BooleanType(), True),
        StructField("type", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("price_string", StringType(), True),
        StructField("image", StringType(), True),
        StructField("webp_image", StringType(), True),
        StructField("number_of_images", IntegerType(), True),
        StructField("avatar", StringType(), True),
        StructField("property_legal_document", IntegerType(), True),
        StructField("size", FloatType(), True),
        StructField("region_v2", IntegerType(), True),
        StructField("area_v2", IntegerType(), True),
        StructField("ward", IntegerType(), True),
        StructField("ward_name", StringType(), True),
        StructField("direction", IntegerType(), True),
        StructField("price_million_per_m2", FloatType(), True),
        StructField("land_type", IntegerType(), True),
        StructField("width", FloatType(), True),
        StructField("length", FloatType(), True),
        StructField("contain_videos", IntegerType(), True),
        StructField("location", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("phone_hidden", BooleanType(), True),
        StructField("owner", BooleanType(), True),
        StructField("zero_deposit", BooleanType(), True),
        StructField("ad_labels", StringType(), True),
        StructField("special_display", BooleanType(), True),
        StructField("has_video", BooleanType(), True),
        StructField("shop_alias", StringType(), True)
    ])

    df = spark.createDataFrame(docs,schema)


    return df



def fillna_cols(df):
    schema = df.schema 
    numeric_cols = [field.name for field in schema.fields if isinstance(field.dataType,NumericType)]
    for col in numeric_cols : 
        df = df.fillna({col: 0})
    string_cols = [field.name for field in schema.fields if isinstance(field.dataType,StringType)]
    for col in string_cols : 
        df = df.fillna({col: 'Unknown '+col})
    boolean_cols = [field.name for field in schema.fields if isinstance(field.dataType,BooleanType)]
    for col in boolean_cols : 
        df = df.fillna({col: False})    
    return df



def preprocessing(df,ward_df,area_df,category_df,region_df) : 

    filtered_df = df.filter(
        ~(sf.col("ward").isNull() ) &
        ~(sf.col("area").isNull() ) &
        ~(sf.col("category").isNull()) &
        ~(sf.col("region").isNull() )
    )
    filled_df = filtered_df.join(ward_df, how='inner',on ='ward').drop(filtered_df.ward_name)
    filled_df = filled_df.join(area_df, how='inner',on ='area').drop(filled_df.area_name)
    filled_df = filled_df.join(region_df, how='inner',on ='region').drop(filled_df.region_name)
    print("--------------------------------------------------------------------------")
    print('Fill null values')
    full_filled_df = fillna_cols(filled_df)
    print("--------------------------------------------------------------------------")
    print('update orig_list_time')
    full_filled_df = full_filled_df.withColumn("orig_list_time",sf.when(sf.col("orig_list_time") == 0, sf.col("list_time")).otherwise(sf.col("orig_list_time")))
    print("--------------------------------------------------------------------------")
    print('Create date_string columns based on list_time')
    final_df = full_filled_df.withColumn('date',sf.date_format((sf.col('list_time')/1000).cast('timestamp'),'yyyy-MM-dd HH:mm:ss')).orderBy('list_time',ascending=False)
    return final_df 


def land_datalake(docs,ward_df,area_df,category_df,region_df) : 
    print("--------------------------------------------------------------------------")
    print('Preprocess data before convert into Spark DataFrame')
    preprocessed_dict = map(preprocessing_col,docs)
    final_docs = list(preprocessed_dict)
    print("--------------------------------------------------------------------------")
    print('Create spark Dataframe')
    initial_df = create_spark(final_docs)
    print("--------------------------------------------------------------------------")
    print('Start preprocessing office data')
    process_df = preprocessing(initial_df,ward_df,area_df,category_df,region_df)
    return process_df

def read_info(table_name) : 
    url = "jdbc:sqlserver://DESKTOP-301A075\DATA_WAREHOUSE:1433;databaseName=data_warehouse"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    info_df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    return info_df

def write_to_SQLServer(df,table_name) : 
    url = "jdbc:sqlserver://DESKTOP-301A075\DATA_WAREHOUSE:1433;databaseName=data_warehouse"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)

    return print('Data imported SQL server successfully')

def get_the_latest_time(type,col_name,myclient) : 
    pipeline = [
        {
            "$group": {
                "_id": None,
                "latest_list_time": {"$max": "$list_time"},
                "latest_orig_list_time": {"$max": "$orig_list_time"}
            }
        }
    ]
    db_name = type +'_real_estate_datalake'
    myDB  = myclient[db_name]
    col = myDB[col_name]
    result = list(col.aggregate(pipeline))
    latest_time = (max(result[0].get('latest_list_time'),result[0].get('latest_orig_list_time') ))
    date_string = datetime.datetime.fromtimestamp(latest_time/1000).strftime('%Y-%m-%d %H:%M:%S')
    return (date_string)
def get_sql_latest_time(table_name) : 
    url = "jdbc:sqlserver://DESKTOP-301A075\DATA_WAREHOUSE:1433;databaseName=data_warehouse"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    mysql_time = spark.read.jdbc(url=url, table=table_name, properties=properties)
    mysql_time = (mysql_time.select('date').agg({'date':'max'}).take(1)[0][0])
    if mysql_time is None : 
        return '1900-01-01 00:00:00'
    else :
        return mysql_time

def main(myclient,type,collection_name,mssql_time) : 
    print("--------------------------------------------------------------------------")
    print('Extracting data from MongoDB')
    db_name = type+'_real_estate_datalake'
    myDB = myclient[db_name]
    col = myDB[collection_name]
    docs = list(col.find())
    print("--------------------------------------------------------------------------")
    print('Extracting information data from Data Warehouse')
    ward_df = read_info('ward_info')
    area_df = read_info('area_info')
    region_df = read_info('region_info')
    category_df = read_info('category_info')
    print("--------------------------------------------------------------------------")
    print('Preprocess MongoDB data')
    final_df = land_datalake(docs,ward_df,area_df,category_df,region_df)
    final_df = final_df.dropDuplicates(subset=['ad_id'])
    print("--------------------------------------------------------------------------")
    print('Extract updated data')
    final_df = final_df.filter(final_df.date > mssql_time)
    final_df.printSchema()
    print("--------------------------------------------------------------------------")
    print('Finish preprocessing data')
    return final_df

if __name__ == "__main__" : 

    spark = SparkSession.builder \
        .getOrCreate()
    print("--------------------------------------------------------------------------")
    print('Connect to MongoDB')
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    print("--------------------------------------------------------------------------")
    print('Start process sell land data')
    mongodb_latest_time = get_the_latest_time('sell','land',myclient)
    mssql_time = get_sql_latest_time('sell_land_data')
    print(mongodb_latest_time)
    print(mssql_time)
    if mongodb_latest_time <= mssql_time : 
        print("No new data")
    else : 
        final_sell_df = main(myclient,'sell','land',mssql_time)

        final_sell_df.show()

        print("--------------------------------------------------------------------------")
        print('Write sell land data to SQL Server')
        write_to_SQLServer(final_sell_df,'sell_land_data')

    print("--------------------------------------------------------------------------")
    print('Start process rent land data')
    mongodb_latest_time = get_the_latest_time('rent','land',myclient)
    mssql_time = get_sql_latest_time('rent_land_data')
    if mongodb_latest_time <= mssql_time : 
        print("No new data")
    else : 
        final_rent_df = main(myclient,'rent','land',mssql_time)

        final_rent_df.show()

        print("--------------------------------------------------------------------------")
        print('Write rent land data to SQL Server')
        write_to_SQLServer(final_rent_df,'rent_land_data')
