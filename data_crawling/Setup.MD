# Setting Up MongoDB in Docker and Running the Crawl Script

## Step 1: Set Up MongoDB Using Docker

1. **Install Docker**

   If Docker is not installed, download and install Docker Desktop from [Docker's official site](https://www.docker.com/products/docker-desktop).

2. **Pull the MongoDB Docker Image**
```
docker pull mongodb/mongodb-community-server:latest
```
3. **Run the MongoDB Docker Container**
```
docker run --name mongodb -p 27017:27017 -d mongodb/mongodb-community-server:latest
```

## Step 2: Run crawler.py file to call API from website to extract needed data
1. **Install Dependencies**
- Use `pip install -r requirements.txt` to install the required libraries to run crawler file
2. **Run file** 
```
python crawler.py
``` 

3. **Crawler.py** 
The `crawler.py` script is designed to crawl real estate data from a public API, process the data, and insert it into a MongoDB database. It consists of several functions that handle different aspects of the data retrieval and storage process : 
Loop for each category of real estate property : 
- 1. Get all data from APIs
- 2. Extract new data which is not existed in MongoDB
- 3. Insert to MongoDB

This script automates the process of keeping the MongoDB database updated with the latest real estate data from an external API.