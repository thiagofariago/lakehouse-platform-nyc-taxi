#!/usr/bin/env python3
"""
NYC Taxi Data Ingestion - BULK MODE
Ingere múltiplos tipos de taxi (yellow, green, fhv, fhvhv) em paralelo

Uso:
  # Ingerir todos os tipos para 2023-01
  export NYC_TAXI_YEAR=2023
  export NYC_TAXI_MONTH=01
  python ingest_spark_bulk.py

  # Ingerir apenas tipos específicos
  export NYC_TAXI_COLORS="yellow,green"
  python ingest_spark_bulk.py
"""

import os
import sys
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
import tempfile
import uuid
import boto3

# Configurações via ENV
YEAR = os.getenv('NYC_TAXI_YEAR', '2023')
MONTH = os.getenv('NYC_TAXI_MONTH', '03')
COLORS = os.getenv('NYC_TAXI_COLORS', 'yellow,green,fhv,fhvhv').split(',')
OVERWRITE = os.getenv('NYC_TAXI_OVERWRITE', 'false').lower() == 'true'

# Mapeamento de tipo para tabela
TABLE_MAP = {
    'yellow': 'yellow_trips',
    'green': 'green_trips',
    'fhv': 'fhv_trips',
    'fhvhv': 'fhvhv_trips'
}


def create_spark_session():
    """Cria SparkSession com configuração Iceberg"""
    print(f"\n{'='*80}")
    print("INITIALIZING SPARK SESSION")
    print(f"{'='*80}")

    spark = SparkSession.builder \
        .appName("NYC Taxi Bulk Ingestion") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

    print("✓ Spark session created successfully")
    return spark


def partition_exists(spark, table_name, year, month):
    """Verifica se partição existe"""
    try:
        result = spark.sql(f"""
            SELECT COUNT(*) as count
            FROM {table_name}
            WHERE year = {year} AND month = {month}
        """).collect()[0]
        return result['count']
    except Exception:
        return 0


def delete_partition(spark, table_name, year, month):
    """Deleta partição para overwrite"""
    try:
        spark.sql(f"""
            DELETE FROM {table_name}
            WHERE year = {year} AND month = {month}
        """)
        print(f"  ✓ Partition deleted: year={year}, month={month}")
        return True
    except Exception as e:
        print(f"  ✗ Error deleting partition: {e}")
        return False


def ingest_single_color(spark, color, year, month, overwrite):
    """
    Ingere um único tipo de taxi
    Retorna: (success, records_count, message)
    """
    table_name = f"iceberg.raw.{TABLE_MAP[color]}"
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    filename = f"{color}_tripdata_{year}-{month}.parquet"
    url = f"{base_url}/{filename}"

    print(f"\n{'─'*80}")
    print(f"📦 Processing: {color.upper()} - {year}-{month}")
    print(f"{'─'*80}")
    print(f"URL: {url}")

    # 1. Validar se partição existe
    existing_records = partition_exists(spark, table_name, year, month)

    if existing_records > 0:
        print(f"⚠️  Partition exists ({existing_records:,} records)")
        if overwrite:
            print(f"🔄 Overwrite mode - deleting partition...")
            if not delete_partition(spark, table_name, year, month):
                return False, 0, "Failed to delete partition"
        else:
            print(f"⏭️  Skip mode - partition already loaded")
            return True, existing_records, "Skipped (already exists)"

    s3_tmp_path = None

    try:
        # 2. Download to local temp
        print(f"⬇️  Downloading...")
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        size_mb = len(response.content) / (1024*1024)
        print(f"✓ Downloaded {size_mb:.2f} MB")

        # 3. Upload to S3 using boto3 (accessible by all Spark executors)
        tmp_id = uuid.uuid4().hex
        s3_tmp_path = f"s3a://lakehouse/tmp/{tmp_id}.parquet"
        s3_key = f"tmp/{tmp_id}.parquet"

        print(f"📤 Uploading to S3...")
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123'
        )
        s3_client.put_object(
            Bucket='lakehouse',
            Key=s3_key,
            Body=response.content
        )
        print(f"✓ Uploaded to {s3_tmp_path}")

        # 4. Read from S3 with Spark (distributed read)
        print(f"📖 Reading from S3 with Spark...")
        df = spark.read.parquet(s3_tmp_path)
        record_count = df.count()
        print(f"✓ Loaded {record_count:,} records")

        # 5. Add metadata
        df_with_metadata = df \
            .withColumn("year", lit(int(year))) \
            .withColumn("month", lit(int(month))) \
            .withColumn("loaded_at", current_timestamp())

        # 6. Write to Iceberg
        print(f"💾 Writing to Iceberg: {table_name}")
        df_with_metadata.write \
            .format("iceberg") \
            .mode("append") \
            .option("write.spark.accept-any-schema", "true") \
            .partitionBy("year", "month") \
            .saveAsTable(table_name)

        print(f"✅ SUCCESS - {record_count:,} records ingested")

        # Cleanup S3 temp file
        if s3_tmp_path:
            try:
                s3_client.delete_object(Bucket='lakehouse', Key=s3_key)
                print(f"✓ Temp S3 file cleaned")
            except Exception as e:
                print(f"⚠️  Failed to clean S3 temp: {e}")

        return True, record_count, "Success"

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"⚠️  File not found (404) - skipping")
            return True, 0, "File not found (404)"
        else:
            print(f"❌ HTTP Error: {e}")
            # Cleanup S3 temp if exists
            if s3_tmp_path:
                try:
                    s3_client = boto3.client(
                        's3',
                        endpoint_url='http://minio:9000',
                        aws_access_key_id='minioadmin',
                        aws_secret_access_key='minioadmin123'
                    )
                    s3_client.delete_object(Bucket='lakehouse', Key=s3_key)
                except:
                    pass
            return False, 0, f"HTTP Error: {e.response.status_code}"

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        # Cleanup S3 temp if exists
        if s3_tmp_path:
            try:
                s3_client = boto3.client(
                    's3',
                    endpoint_url='http://minio:9000',
                    aws_access_key_id='minioadmin',
                    aws_secret_access_key='minioadmin123'
                )
                s3_client.delete_object(Bucket='lakehouse', Key=s3_key)
            except:
                pass
        return False, 0, f"Error: {str(e)}"


def main():
    """Pipeline principal - ingere múltiplos tipos de taxi"""
    print(f"{'='*80}")
    print("NYC TAXI BULK INGESTION")
    print(f"{'='*80}")
    print(f"Started at: {datetime.now()}")
    print(f"Period: {YEAR}-{MONTH}")
    print(f"Colors to process: {', '.join(COLORS)}")
    print(f"Overwrite mode: {OVERWRITE}")

    # Validar cores
    invalid_colors = [c for c in COLORS if c not in TABLE_MAP]
    if invalid_colors:
        print(f"\n❌ Invalid colors: {invalid_colors}")
        print(f"Valid options: {list(TABLE_MAP.keys())}")
        sys.exit(1)

    # Criar Spark session
    spark = create_spark_session()

    # Processar cada cor
    results = {}
    for color in COLORS:
        success, records, message = ingest_single_color(
            spark, color, YEAR, MONTH, OVERWRITE
        )
        results[color] = {
            'success': success,
            'records': records,
            'message': message
        }

    # Resumo final
    print(f"\n{'='*80}")
    print("INGESTION SUMMARY")
    print(f"{'='*80}")
    print(f"Period: {YEAR}-{MONTH}")
    print(f"Finished at: {datetime.now()}\n")

    total_success = 0
    total_failed = 0
    total_records = 0

    print(f"{'Color':<10} {'Status':<15} {'Records':>15} {'Message':<30}")
    print(f"{'─'*10} {'─'*15} {'─'*15} {'─'*30}")

    for color, result in results.items():
        status = "✅ Success" if result['success'] else "❌ Failed"
        records_str = f"{result['records']:,}" if result['records'] > 0 else "-"

        print(f"{color:<10} {status:<15} {records_str:>15} {result['message']:<30}")

        if result['success']:
            total_success += 1
            total_records += result['records']
        else:
            total_failed += 1

    print(f"{'─'*10} {'─'*15} {'─'*15} {'─'*30}")
    print(f"{'TOTAL':<10} {f'{total_success} OK, {total_failed} FAIL':<15} {total_records:>15,}")

    print(f"\n{'='*80}")
    if total_failed == 0:
        print("✅ ALL INGESTIONS COMPLETED SUCCESSFULLY")
        exit_code = 0
    else:
        print(f"⚠️  COMPLETED WITH {total_failed} FAILURES")
        exit_code = 1
    print(f"{'='*80}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
