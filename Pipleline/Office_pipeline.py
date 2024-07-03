import pymongo
import pyspark.sql.functions as sf
from uuid import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from uuid import * 
from pyspark.sql.window import Window as W
import findspark
from pyspark.sql.types import NumericType
findspark.init()
findspark.find()
def preprocessing_col(element) : 
    full_col = ['ad_id', 'list_id', 'list_time', 'orig_list_time', 'date', 'account_id', 'account_oid', 'account_name', 'state', 'subject', 'body', 
                'category', 'category_name', 'area', 'area_name', 'region', 'region_name', 'company_ad', 'type', 'price', 'price_string', 'image', 'webp_image', 
                'special_display', 'number_of_images', 'avatar', 'property_legal_document', 'commercial_type', 'size', 'region_v2', 'area_v2', 'ward', 'ward_name', 
                'direction', 'price_million_per_m2', 'furnishing_sell', 'contain_videos', 'location', 'longitude', 
                'latitude', 'phone_hidden', 'owner', 'zero_deposit', 'has_video', 'shop_alias', 'streetnumber_display', 'address', 'block', 'label_campaigns']
    
    for col in full_col : 
        if col not in element.keys() : 
            element[col] = None
    
    numeric_col = ['size', 'price_million_per_m2', 'longitude', 'latitude','list_time','orig_list_time','price']

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
        StructField("special_display", BooleanType(), True),
        StructField("number_of_images", IntegerType(), True),
        StructField("avatar", StringType(), True),
        StructField("property_legal_document", IntegerType(), True),
        StructField("commercial_type", IntegerType(), True),
        StructField("size", FloatType(), True),
        StructField("region_v2", IntegerType(), True),
        StructField("area_v2", IntegerType(), True),
        StructField("ward", IntegerType(), True),
        StructField("ward_name", StringType(), True),
        StructField("direction", IntegerType(), True),
        StructField("price_million_per_m2", FloatType(), True),
        StructField("furnishing_sell", IntegerType(), True),
        StructField("contain_videos", IntegerType(), True),
        StructField("location", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("phone_hidden", BooleanType(), True),
        StructField("owner", BooleanType(), True),
        StructField("zero_deposit", BooleanType(), True),
        StructField("has_video", BooleanType(), True),
        StructField("shop_alias", StringType(), True),
        StructField("streetnumber_display", IntegerType(), True),
        StructField("address", StringType(), True),
        StructField("block", StringType(), True),
        StructField("label_campaigns", StringType(), True),
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
    mode_furnish = df.dropna(subset='furnishing_sell').groupBy('furnishing_sell').count().orderBy('count',ascending=False).first()[0]
    df = df.fillna({'furnishing_sell' :mode_furnish})
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
    final_df = full_filled_df.withColumn('date',sf.date_format((sf.col('list_time')/1000).cast('timestamp'),'yyyy-MM-dd')).orderBy('list_time',ascending=False)
    return final_df 


def office_datalake(docs,ward_df,area_df,category_df,region_df) : 
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

def main(myclient,type,collection_name) : 
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
    final_df = office_datalake(docs,ward_df,area_df,category_df,region_df)
    final_df = final_df.dropDuplicates(subset=['ad_id'])
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
    print('Start process sell office data')
    final_sell_df = main(myclient,'sell','office')

    final_sell_df.show()

    print("--------------------------------------------------------------------------")
    print('Write sell office data to SQL Server')
    write_to_SQLServer(final_sell_df,'sell_office_data')

    print("--------------------------------------------------------------------------")
    print('Start process rent office data')
    final_rent_df = main(myclient,'rent','office')

    final_rent_df.show()

    print("--------------------------------------------------------------------------")
    print('Write rent office data to SQL Server')
    write_to_SQLServer(final_rent_df,'rent_office_data')



    