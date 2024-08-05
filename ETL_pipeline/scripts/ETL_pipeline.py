import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from uuid import * 
from pyspark.sql.window import Window as W



import datetime

def write_to_SQLServer(df,table_name) : 
    url = "jdbc:sqlserver://DESKTOP-301A075\DATA_WAREHOUSE:1433;databaseName=data_warehouse"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)
def read_info(table_name) : 
    url = "jdbc:sqlserver://DESKTOP-301A075\DATA_WAREHOUSE:1433;databaseName=data_warehouse"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "charset": "UTF-8" 
    }
    info_df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    return info_df
def preprocess_type(df): 
    new_df = df.withColumn('type_name', sf.when(sf.col('type') == 's','Mua bán')
                                        .when(sf.col('type') == 'u','Thuê'))
    return new_df




def preprocess_house(raw_df): 
    pre_df = raw_df.withColumn('month',sf.month(sf.col('date')))
    pre_df = pre_df.withColumn('year',sf.year(sf.col('date')))
    pre_df = pre_df.withColumn('day_of_week',sf.dayofweek(sf.col('date')))
    pre_df = pre_df.withColumn('day',sf.dayofmonth(sf.col('date')))
    pre_df = pre_df.withColumn('date',sf.date_format(sf.to_timestamp(sf.col('date'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd'))
    pre_df = pre_df.withColumn('day_of_week', 
                   sf.when(sf.col('day_of_week') == 1, 'Sunday')
                     .when(sf.col('day_of_week') == 2, 'Monday')
                     .when(sf.col('day_of_week') == 3, 'Tuesday')
                     .when(sf.col('day_of_week') == 4, 'Wednesday')
                     .when(sf.col('day_of_week') == 5, 'Thursday')
                     .when(sf.col('day_of_week') == 6, 'Friday')
                     .when(sf.col('day_of_week') == 7, 'Saturday')
                     .otherwise('Unknown'))
    pre_df = pre_df.withColumnRenamed('category_name','property_type')
    pre_df = pre_df.withColumnRenamed('area_name','area')
    pre_df = pre_df.withColumnRenamed('ward_name','ward')
    pre_df = pre_df.withColumnRenamed('region_name','region')
    pre_df = pre_df.withColumn('property_subtype', sf.when(sf.col('house_type') == 1,'Nhà mặt phố, mặt tiền')
                                            .when(sf.col('house_type') == 2,'Nhà biệt thự')
                                            .when(sf.col('house_type') == 3,'Nhà ngõ, hẻm')
                                            .when(sf.col('house_type') == 4,'Nhà phố liền kề')        )
    return pre_df
                                            


def ETL(df,date_info,region_info,property_info,type_info) : 
    df = df.join(date_info,on=['date','day','month','year','day_of_week'],how='inner') \
        .join(type_info,on=['type_name'],how='inner') \
        .join(region_info,on=['region','area','ward'],how='inner')\
        .join(property_info,on=['property_type','property_subtype'],how='inner')
    new_df = df.select('property_subtype_key','property_type_key','date_key','type_key','region_key','size','ad_id','price','price_million_per_m2')
    process_df = new_df.groupBy('property_subtype_key', 'property_type_key', 'date_key', 'type_key', 'region_key') \
    .agg(
        sf.count('ad_id').alias('number_of_ad'),  
        sf.round(sf.mean('price'), 3).alias('avg_price'),    
        sf.round(sf.mean('size'), 3).alias('avg_size'),  
        sf.round(sf.mean('price_million_per_m2'), 3).alias('avg_price_per_m2') 
    )
    return process_df

def main_house(dw_time) :
    print("---------------------------------------------------------------------------------------")
    print("Extracting house data from Database")
    raw_sell_df = read_info('sell_house_data')
    sell_df = raw_sell_df.select('ad_id','region_name','category_name','area_name','ward_name','type','size','house_type','price','price_million_per_m2','date')
    sell_df = sell_df.filter(sf.col("date") > dw_time)
    raw_rent_df = read_info('rent_house_data')
    rent_df = raw_rent_df.select('ad_id','region_name','category_name','area_name','ward_name','type','size','house_type','price','price_million_per_m2','date')
    rent_df = rent_df.filter(sf.col("date") > dw_time)
    print("---------------------------------------------------------------------------------------")
    print("Preprocess data")
    preprocessed_sell_df = preprocess_house(sell_df)
    preprocessed_rent_df = preprocess_house(rent_df)
    print("---------------------------------------------------------------------------------------")
    print("Add type of property")
    processed_sell_df = preprocess_type(preprocessed_sell_df)
    processed_sell_df.printSchema()
    processed_rent_df = preprocess_type(preprocessed_rent_df)
    processed_rent_df.printSchema()
    print("---------------------------------------------------------------------------------------")
    print("Get data from all dimension tables")
    region_info = read_info('region_dim')
    property_info = read_info('property_type_dim')
    type_info = read_info('type_dim')
    date_info = read_info('date_dim')
    print("---------------------------------------------------------------------------------------")
    print("Perform ETL Pipeline for house data")
    final_sell_df = ETL(processed_sell_df,date_info,region_info,property_info,type_info)
    final_sell_df.show()
    final_rent_df = ETL(processed_rent_df,date_info,region_info,property_info,type_info)
    final_rent_df.show()
    print("---------------------------------------------------------------------------------------")
    print("Write house data to Fact table in DataWarehouse")
    write_to_SQLServer(final_sell_df,'real_estate_fact')
    write_to_SQLServer(final_rent_df,'real_estate_fact')
    print("---------------------------------------------------------------------------------------")
    print("Finish all tasks")




    
def preprocess_land(raw_df): 
    pre_df = raw_df.withColumn('month',sf.month(sf.col('date')))
    pre_df = pre_df.withColumn('year',sf.year(sf.col('date')))
    pre_df = pre_df.withColumn('day_of_week',sf.dayofweek(sf.col('date')))
    pre_df = pre_df.withColumn('day',sf.dayofmonth(sf.col('date')))
    pre_df = pre_df.withColumn('date',sf.date_format(sf.to_timestamp(sf.col('date'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd'))
    pre_df = pre_df.withColumn('day_of_week', 
                   sf.when(sf.col('day_of_week') == 1, 'Sunday')
                     .when(sf.col('day_of_week') == 2, 'Monday')
                     .when(sf.col('day_of_week') == 3, 'Tuesday')
                     .when(sf.col('day_of_week') == 4, 'Wednesday')
                     .when(sf.col('day_of_week') == 5, 'Thursday')
                     .when(sf.col('day_of_week') == 6, 'Friday')
                     .when(sf.col('day_of_week') == 7, 'Saturday')
                     .otherwise('Unknown'))
    pre_df = pre_df.withColumnRenamed('category_name','property_type')
    pre_df = pre_df.withColumnRenamed('area_name','area')
    pre_df = pre_df.withColumnRenamed('ward_name','ward')
    pre_df = pre_df.withColumnRenamed('region_name','region')
    pre_df = pre_df.withColumn('property_subtype', sf.when(sf.col('land_type') == 1,'Đất thổ cư')
                                            .when(sf.col('land_type') == 2,'Đất nền dự án')
                                            .when(sf.col('land_type') == 3,'Đất công nghiệp')
                                            .when(sf.col('land_type') == 4,'Đất nông nghiệp')        )
    return pre_df
    
    
def main_land(dw_time) :
    print("---------------------------------------------------------------------------------------")
    print("Extracting land data from Database")
    raw_sell_df = read_info('sell_land_data')
    sell_df = raw_sell_df.select('ad_id','region_name','category_name','area_name','ward_name','type','size','land_type','price','price_million_per_m2','date')
    sell_df = sell_df.filter(sf.col("date") > dw_time)
    raw_rent_df = read_info('rent_land_data')
    rent_df = raw_rent_df.select('ad_id','region_name','category_name','area_name','ward_name','type','size','land_type','price','price_million_per_m2','date')
    rent_df = rent_df.filter(sf.col("date") > dw_time)
    print("---------------------------------------------------------------------------------------")
    print("Preprocess data")
    preprocessed_sell_df = preprocess_land(sell_df)
    preprocessed_rent_df = preprocess_land(rent_df)
    print("---------------------------------------------------------------------------------------")
    print("Add type of property")
    processed_sell_df = preprocess_type(preprocessed_sell_df)
    processed_sell_df.printSchema()
    processed_rent_df = preprocess_type(preprocessed_rent_df)
    processed_rent_df.printSchema()
    print("---------------------------------------------------------------------------------------")
    print("Get data from all dimension tables")
    region_info = read_info('region_dim')
    property_info = read_info('property_type_dim')
    type_info = read_info('type_dim')
    date_info = read_info('date_dim')
    print("---------------------------------------------------------------------------------------")
    print("Perform ETL Pipeline for land data")
    final_sell_df = ETL(processed_sell_df,date_info,region_info,property_info,type_info)
    final_sell_df.show()
    final_rent_df = ETL(processed_rent_df,date_info,region_info,property_info,type_info)
    final_rent_df.show()
    print("---------------------------------------------------------------------------------------")
    print("Write land data to Fact table in DataWarehouse")
    write_to_SQLServer(final_sell_df,'real_estate_fact')
    write_to_SQLServer(final_rent_df,'real_estate_fact')
    print("---------------------------------------------------------------------------------------")
    print("Finish all tasks")



def preprocess_office(raw_df): 
    pre_df = raw_df.withColumn('month',sf.month(sf.col('date')))
    pre_df = pre_df.withColumn('year',sf.year(sf.col('date')))
    pre_df = pre_df.withColumn('day_of_week',sf.dayofweek(sf.col('date')))
    pre_df = pre_df.withColumn('day',sf.dayofmonth(sf.col('date')))
    pre_df = pre_df.withColumn('date',sf.date_format(sf.to_timestamp(sf.col('date'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd'))
    pre_df = pre_df.withColumn('day_of_week', 
                   sf.when(sf.col('day_of_week') == 1, 'Sunday')
                     .when(sf.col('day_of_week') == 2, 'Monday')
                     .when(sf.col('day_of_week') == 3, 'Tuesday')
                     .when(sf.col('day_of_week') == 4, 'Wednesday')
                     .when(sf.col('day_of_week') == 5, 'Thursday')
                     .when(sf.col('day_of_week') == 6, 'Friday')
                     .when(sf.col('day_of_week') == 7, 'Saturday')
                     .otherwise('Unknown'))
    pre_df = pre_df.withColumnRenamed('category_name','property_type')
    pre_df = pre_df.withColumnRenamed('area_name','area')
    pre_df = pre_df.withColumnRenamed('ward_name','ward')
    pre_df = pre_df.withColumnRenamed('region_name','region')
    pre_df = pre_df.withColumn('property_subtype', sf.when(sf.col('commercial_type') == 1,'Shophouse')
                                            .when(sf.col('commercial_type') == 2,'Officetel')
                                            .when(sf.col('commercial_type') == 3,'Văn phòng')
                                            .when(sf.col('commercial_type') == 4,'Mặt bằng kinh doanh')        )
    return pre_df

def main_office(dw_time) : 
    print("---------------------------------------------------------------------------------------")
    print("Extracting office data from Database")
    raw_sell_df = read_info('sell_office_data')
    sell_df = raw_sell_df.select('ad_id','region_name','category_name','area_name','ward_name','type','size','commercial_type','price','price_million_per_m2','date')
    sell_df = sell_df.withColumn("date", sf.to_date(sf.col("date"), "yyyy-MM-dd"))
    sell_df = sell_df.filter(sf.col("date") > dw_time)
    raw_rent_df = read_info('rent_office_data')
    rent_df = raw_rent_df.select('ad_id','region_name','category_name','area_name','ward_name','type','size','commercial_type','price','price_million_per_m2','date')
    rent_df = rent_df.withColumn("date", sf.to_date(sf.col("date"), "yyyy-MM-dd"))
    rent_df = rent_df.filter(sf.col("date") > dw_time)
    print("---------------------------------------------------------------------------------------")
    print("Preprocess data")
    preprocessed_sell_df = preprocess_office(sell_df)
    preprocessed_rent_df = preprocess_office(rent_df)
    print("---------------------------------------------------------------------------------------")
    print("Add type of property")
    processed_sell_df = preprocess_type(preprocessed_sell_df)
    processed_sell_df.printSchema()
    processed_rent_df = preprocess_type(preprocessed_rent_df)
    processed_rent_df.printSchema()
    print("---------------------------------------------------------------------------------------")
    print("Get data from all dimension tables")
    region_info = read_info('region_dim')
    property_info = read_info('property_type_dim')
    type_info = read_info('type_dim')
    date_info = read_info('date_dim')
    print("---------------------------------------------------------------------------------------")
    print("Perform ETL Pipeline for office data")
    final_sell_df = ETL(processed_sell_df,date_info,region_info,property_info,type_info)
    final_sell_df.show()
    final_rent_df = ETL(processed_rent_df,date_info,region_info,property_info,type_info)
    final_rent_df.show()
    print("---------------------------------------------------------------------------------------")
    print("Write office data to Fact table in DataWarehouse")
    write_to_SQLServer(final_sell_df,'real_estate_fact')
    write_to_SQLServer(final_rent_df,'real_estate_fact')
    print("---------------------------------------------------------------------------------------")
    print("Finish all tasks")


def preprocess_apartment(raw_df): 
    pre_df = raw_df.withColumn('month',sf.month(sf.col('date')))
    pre_df = pre_df.withColumn('year',sf.year(sf.col('date')))
    pre_df = pre_df.withColumn('day_of_week',sf.dayofweek(sf.col('date')))
    pre_df = pre_df.withColumn('day',sf.dayofmonth(sf.col('date')))
    pre_df = pre_df.withColumn('date',sf.date_format(sf.to_timestamp(sf.col('date'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd'))
    pre_df = pre_df.withColumn('day_of_week', 
                   sf.when(sf.col('day_of_week') == 1, 'Sunday')
                     .when(sf.col('day_of_week') == 2, 'Monday')
                     .when(sf.col('day_of_week') == 3, 'Tuesday')
                     .when(sf.col('day_of_week') == 4, 'Wednesday')
                     .when(sf.col('day_of_week') == 5, 'Thursday')
                     .when(sf.col('day_of_week') == 6, 'Friday')
                     .when(sf.col('day_of_week') == 7, 'Saturday')
                     .otherwise('Unknown'))
    pre_df = pre_df.withColumnRenamed('category_name','property_type')
    pre_df = pre_df.withColumnRenamed('area_name','area')
    pre_df = pre_df.withColumnRenamed('ward_name','ward')
    pre_df = pre_df.withColumnRenamed('region_name','region')
    pre_df = pre_df.withColumn('property_subtype', sf.when(sf.col('apartment_type') == 1,'Chung cư')
                                            .when(sf.col('apartment_type') == 2,'Căn hộ dịch vụ, mini')
                                            .when(sf.col('apartment_type') == 3,'Duplex')
                                            .when(sf.col('apartment_type') == 4,'Penthouse')     
                                            .when(sf.col('apartment_type') == 5,'Tập thể, cư xá')
                                            .when(sf.col('apartment_type') == 6,'Officetel')       )
    return pre_df

def main_apartment(dw_time) : 
    print("---------------------------------------------------------------------------------------")
    print("Extracting apartment data from Database")
    raw_sell_df = read_info('sell_apartment_data')
    sell_df = raw_sell_df.select('ad_id','region_name','category_name','area_name','ward_name','type','size','apartment_type','price','price_million_per_m2','date')
    sell_df = sell_df.filter(sf.col("date") > dw_time)
    raw_rent_df = read_info('rent_apartment_data')
    rent_df = raw_rent_df.select('ad_id','region_name','category_name','area_name','ward_name','type','size','apartment_type','price','price_million_per_m2','date')
    rent_df = rent_df.filter(sf.col("date") > dw_time)
    print("---------------------------------------------------------------------------------------")
    print("Preprocess data")
    preprocessed_sell_df = preprocess_apartment(sell_df)
    preprocessed_rent_df = preprocess_apartment(rent_df)
    print("---------------------------------------------------------------------------------------")
    print("Add type of property")
    processed_sell_df = preprocess_type(preprocessed_sell_df)
    processed_sell_df.printSchema()
    processed_rent_df = preprocess_type(preprocessed_rent_df)
    processed_rent_df.printSchema()
    print("---------------------------------------------------------------------------------------")
    print("Get data from all dimension tables")
    region_info = read_info('region_dim')
    property_info = read_info('property_type_dim')
    type_info = read_info('type_dim')
    date_info = read_info('date_dim')
    print("---------------------------------------------------------------------------------------")
    print("Perform ETL Pipeline for office data")
    final_sell_df = ETL(processed_sell_df,date_info,region_info,property_info,type_info)
    final_sell_df.show()
    final_rent_df = ETL(processed_rent_df,date_info,region_info,property_info,type_info)
    final_rent_df.show()
    print("---------------------------------------------------------------------------------------")
    print("Write apartment data to Fact table in DataWarehouse")
    write_to_SQLServer(final_sell_df,'real_estate_fact')
    write_to_SQLServer(final_rent_df,'real_estate_fact')
    print("---------------------------------------------------------------------------------------")
    print("Finish all tasks")


def get_sql_latest_time_database(table_name) : 
    url = "jdbc:sqlserver://DESKTOP-301A075\DATA_WAREHOUSE:1433;databaseName=data_warehouse"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    mysql_time = spark.read.jdbc(url=url, table=table_name, properties=properties)
    mysql_time = (mysql_time.select('date').agg({'date':'max'}).take(1)[0][0])
    if mysql_time is None : 
        return datetime.datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    else :
        try:
            latest_time = datetime.datetime.strptime(mysql_time, '%Y-%m-%d %H:%M:%S').date()
            return latest_time
        except :
            return datetime.datetime.strptime(mysql_time, '%Y-%m-%d').date()
        
def get_sql_latest_time_dw(table_name) : 
    url = "jdbc:sqlserver://DESKTOP-301A075\DATA_WAREHOUSE:1433;databaseName=data_warehouse"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    fact_table = spark.read.jdbc(url=url, table=table_name, properties=properties)
    date_dim = spark.read.jdbc(url=url, table='date_dim', properties=properties)
    mysql_time = fact_table.join(date_dim,on='date_key',how='inner') 
    mysql_time = (mysql_time.select('date').agg({'date':'max'}).take(1)[0][0])
    if mysql_time is None : 
        return datetime.datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S').date()
    else :
        return mysql_time
    



if __name__ == '__main__' : 
    spark = SparkSession.builder.getOrCreate()
    database_time = get_sql_latest_time_database('sell_house_data')
    all_table_names = ['rent_house_data','sell_land_data','rent_land_data','sell_apartment_data','rent_apartment_data','sell_office_data','rent_office_data']
    for table in all_table_names : 
        temp_time = get_sql_latest_time_database(table)
        if temp_time < database_time : database_time = temp_time
    dw_time = get_sql_latest_time_dw('real_estate_fact')
    if dw_time >= database_time : print("NO new data")
    else : 
        print('Data Warehouse time :' ,dw_time)
        print('Database time :' ,database_time)
        main_house(dw_time)
        print('Data Warehouse time :' ,dw_time)
        main_land(dw_time)
        print('Data Warehouse time :' ,dw_time)
        main_apartment(dw_time)
        print('Data Warehouse time :' ,dw_time)
        main_office(dw_time)