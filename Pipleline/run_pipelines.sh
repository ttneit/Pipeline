#!/bin/bash

# Define the paths to your scripts
SCRIPTS=(
    "/opt/application/src/pipeline1.py"
    "/opt/application/src/pipeline2.py"
    "/opt/application/src/pipeline3.py"
    "/opt/application/src/pipeline4.py"
)

# Loop through the scripts and execute them
for SCRIPT in "${SCRIPTS[@]}"
do
    echo "Running $SCRIPT..."
    /opt/bitnami/spark/bin/spark-submit "$SCRIPT"
done