import requests
import pandas as pd
import random
import time
import pymongo


def binary_search(data, latest_time) :
    if data[-1]['list_time'] > latest_time : return data
    elif data[0]['list_time'] < latest_time : return []


    low = 0 
    high = len(data)
    while(low <= high) : 
        mid = (high + low)//2
        if data[mid]['list_time'] < latest_time : 
            high = mid -1
        elif data[mid]['list_time'] > latest_time :
            low = mid +1
        else : return data[:mid]
    
    return data[:low]


def crawl_data(code ,name_code,type,latest_time) : 
    error = 0
    user_agents = [ 
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36', 
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36', 
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148', 
    'Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36' 
    ] 
    page = 1
    o=0
    total_num = 0 
    limit=100
    full_data = []
    print(f"Start scrawling {name_code}")
    while True : 
        headers = {'User-Agent': random.choice(user_agents)}
        sam = f"https://gateway.chotot.com/v1/public/ad-listing?limit={limit}&cg={code}&region_v2=13000&o={o}&page={page}&st={type},k&key_param_included=true"
        
        response = requests.get(sam,headers=headers)
        if response.status_code != 200 : 
            error += 1
            if error == 5 : 
                break
        data = response.json()

        number = len(data['ads']) if 'ads' in data else 0
        total_num += number
        page +=1
        o = o + limit

        
        if number == 0  : 
            error +=1
            time.sleep(random.randint(3,7))
            if error == 3: 
                break
        else :
            filtered_data = binary_search(data['ads'],latest_time)
            if len(filtered_data) >  0 : 
                full_data.extend(filtered_data)
            if len(filtered_data) < number : 
                print("The data has been crawled all ")
                break
        time.sleep(random.randint(2,4))

    print(f'Finished crawling {name_code}')
    return full_data


def crawl_task(codes,name_codes,latest_time) : 
    full_data ={}
    full_data['sell'] = {}
    full_data['rent'] = {}
    print("--------------------------------------------------------------------") 
    print('Start crawling for rent real estate')
    for code ,name_code in zip(codes,name_codes) : 
        crawled_data = crawl_data(code ,name_code,'u',latest_time)
        full_data['rent'][name_code] = crawled_data
        print(f"The {name_code} which has the code is {code} has crawled data having length  {len(crawled_data)}")

    print("--------------------------------------------------------------------") 
    print('Start crawling for sell real estate')
    for code ,name_code in zip(codes,name_codes) : 
        crawled_data = crawl_data(code ,name_code,'s',latest_time)
        full_data['sell'][name_code] = crawled_data
        print(f"The {name_code} which has the code is {code} has crawled data having length  {len(crawled_data)}")
    return full_data

def insert_into_mongo(name_code,type,data,myclient) : 
    if len(data) == 0 :
        return print("There is no data to be inserted")
    pre_db = ''
    if type == 'u' : pre_db = 'rent'
    else : pre_db = 'sell'

    name_db = pre_db + '_real_estate_datalake'
    myDB = myclient[name_db]
    code_col = myDB[name_code]
    try:
        print(f"Try inserting data in collection {name_code} in database {name_db}")
        query_insert = code_col.insert_many(data)
        if query_insert.acknowledged:
            print(f"Insert successful. Document IDs: {query_insert.inserted_ids}")
        else:
            print("Insert failed.")
    except pymongo.errors.PyMongoError as e:
        print(f"An error occurred: {e}")


def insert_task (name_codes,types,full_data,myclient) : 
    print("Start inserting data into MongoDB")
    for type in types : 
        if type == 'u' : 
            type_data = full_data['rent']
        else : type_data = full_data['sell']
        for name_code in name_codes : 
            inserted_data = type_data[name_code]
            insert_into_mongo(name_code,type,inserted_data,myclient)
    return print("Finish inserting data into MongoDB")

def get_the_latest_time(name_codes,myclient) : 
    pipeline = [
        {
            "$group": {
                "_id": None,
                "latest_list_time": {"$max": "$list_time"},
                "latest_orig_list_time": {"$max": "$orig_list_time"}
            }
        }
    ]
    latest_time = []
    for name_code in name_codes : 
        myDB  = myclient['sell_real_estate_datalake']
        col = myDB[name_code]
        result = list(col.aggregate(pipeline))
        latest_time.append(max(result[0].get('latest_list_time'),result[0].get('latest_orig_list_time') ))

    for name_code in name_codes : 
        myDB  = myclient['rent_real_estate_datalake']
        col = myDB[name_code]
        result = list(col.aggregate(pipeline))
        latest_time.append(max(result[0].get('latest_list_time'),result[0].get('latest_orig_list_time') ))
    return max(latest_time)

if __name__ == '__main__' : 
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    codes = [1010,1020,1030,1040]
    name_codes = ['apartment','house','office','land']
    types = ['s','u']
    latest_time = get_the_latest_time(name_codes,myclient)
    full_data = crawl_task(codes,name_codes,latest_time)
    insert_task(name_codes,types,full_data,myclient)
