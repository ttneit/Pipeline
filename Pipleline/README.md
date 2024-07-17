# Steps to Build and Run the Docker Container
1. Build Docker Image
```
docker build -t pipelines .
```

2. Run the Docker container
```
docker run --network="host" --name real_estate_pipelines pipelines
```
