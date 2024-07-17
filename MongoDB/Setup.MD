# Set up MongoDB in Docker
1. Pull the latest MongoDB Docker Image
```
docker pull mongodb/mongodb-community-server:latest
```
2. Run the MongoDB Docker Container
```
docker run --name mongodb -p 27017:27017 -d mongodb/mongodb-community-server:latest
```

## Run crawler.py file to call API from website to extract needed data
1. Install Dependencies
- Use `pip install -r requirements.txt` to install the required libraries to run crawler file
2. Run file : 
```
python crawler.py
``` 

### Crawler File : 
This file will trigger API from online website then extract the latest data from the returned data then insert into MongoDB