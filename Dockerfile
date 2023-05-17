FROM openjdk:8-jdk-alpine

# Set the Spark environment variables
ENV SPARK_VERSION 3.3.2
ENV HADOOP_VERSION 3

# Install Python and necessary Python libraries
RUN apk add --update python3 py3-pip && \
    pip3 install --upgrade pip && \
    pip3 install pyspark==3.1.2 delta-spark==2.3.0 python-dotenv py4j --no-dependencies
# # Copy and install Python dependencies
# COPY requirements.txt .
# RUN pip3 install --no-cache-dir -r requirements.txt

# Install Spark
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN apk add --no-cache bash

# Set the working directory and copy the Python script
WORKDIR /spark
COPY code/green.py .

# Copy the input CSV files to the appropriate location inside the container
COPY code/file_with_header /spark/data/file_with_header/
COPY code/file_without_header /spark/data/file_without_header/

# Set the entrypoint
CMD ["/spark/bin/spark-submit", \
    "--packages", "io.delta:delta-core_2.12:2.3.0", \
    "--class", "org.apache.spark.deploy.SparkSubmit", \
    "--master", "local[*]", \
    "--name", "Ingest CSV to DeltaLake", \
    "/spark/green.py"] 