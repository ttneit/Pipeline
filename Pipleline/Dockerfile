# Use the official Spark base image
FROM bitnami/spark:latest

# Install python libraries
RUN pip install --upgrade pip

COPY requirements.txt /opt/application/requirements.txt
RUN pip install -r /opt/application/requirements.txt

# Copy your python scripts
COPY src/ /opt/application/src/

# Copy the script to run all pipelines
COPY run_pipelines.sh /opt/application/run_pipelines.sh

# Set the working directory
WORKDIR /opt/application

# Make the script executable
RUN chmod +x /opt/application/run_pipelines.sh

# Command to run the script
CMD ["./run_pipelines.sh"]
