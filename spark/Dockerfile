FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

# Install wget for downloading JARs
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Download Iceberg Spark Runtime JAR
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# Download AWS SDK for S3 support
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Install Python dependencies
RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    pyarrow==12.0.1 \
    requests==2.31.0 \
    boto3==1.26.137

USER ${spark_uid}

WORKDIR /opt/spark
