#!/bin/bash
# Edit the variables below to change what data to ingest

set -e

# CONFIGURATION
export NYC_TAXI_YEAR=2023
export NYC_TAXI_MONTH=02
export NYC_TAXI_COLORS=yellow,green,fhv,fhvhv
export NYC_TAXI_OVERWRITE=false

echo "=================================="
echo "Spark Bulk Ingestion Wrapper"
echo "=================================="
echo "Year: $NYC_TAXI_YEAR"
echo "Month: $NYC_TAXI_MONTH"
echo "Colors: $NYC_TAXI_COLORS"
echo "Overwrite: $NYC_TAXI_OVERWRITE"
echo ""

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  /opt/scripts/nyc_taxi/ingest_spark_bulk.py

echo ""
echo "Bulk ingestion completed"
