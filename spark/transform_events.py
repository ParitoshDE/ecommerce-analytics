"""
transform_events.py — PySpark batch transformation for Olist Brazilian E-Commerce.

Reads raw Olist CSV files from GCS (or local data/raw/), joins all 8 tables
into a single denormalized orders-enriched dataset, and writes partitioned
Parquet for BigQuery load.

Output schema (grain = one row per order item):
    order_id, order_item_id, order_status,
    order_purchase_timestamp, order_purchase_date,
    order_purchase_year, order_purchase_month,
    order_delivered_customer_date, order_estimated_delivery_date,
    delivery_days, estimated_delivery_days, is_on_time,
    customer_id, customer_unique_id, customer_city, customer_state,
    product_id, product_category_name, product_category_name_english,
    seller_id, seller_city, seller_state,
    price, freight_value, total_item_value,
    payment_type, payment_value, review_score
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


def get_spark(enable_gcs: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("olist-ecommerce-transform")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )

    if enable_gcs:
        builder = (
            builder
            .config(
                "spark.jars.packages",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26",
            )
            .config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            )
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            )
        )

        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_path:
            norm = credentials_path.replace("\\", "/")
            builder = (
                builder
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", norm)
                .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
                .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", norm)
            )

    return builder.getOrCreate()


def write_with_pyarrow_fallback(df_final, output_path: str, batch_size: int = 50000) -> None:
    """Fallback writer for Windows environments where Hadoop native IO is unavailable."""
    out_dir = Path(output_path)
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows_batch: list[dict] = []
    rows_written = 0
    file_index = 0

    for row in df_final.toLocalIterator():
        rows_batch.append(row.asDict(recursive=True))
        if len(rows_batch) >= batch_size:
            table = pa.Table.from_pylist(rows_batch)
            pq.write_table(table, out_dir / f"part-{file_index:05d}.parquet", compression="snappy")
            rows_written += len(rows_batch)
            rows_batch = []
            file_index += 1

    if rows_batch:
        table = pa.Table.from_pylist(rows_batch)
        pq.write_table(table, out_dir / f"part-{file_index:05d}.parquet", compression="snappy")
        rows_written += len(rows_batch)

    print(f"[spark] PyArrow fallback: {rows_written:,} rows written to {output_path}")


def transform(spark: SparkSession, raw_prefix: str, output_path: str) -> None:
    def read_csv(filename: str):
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{raw_prefix}/{filename}")
        )

    # --- Load all 8 Olist CSV tables ---
    orders    = read_csv("olist_orders_dataset.csv")
    items     = read_csv("olist_order_items_dataset.csv")
    products  = read_csv("olist_products_dataset.csv")
    customers = read_csv("olist_customers_dataset.csv")
    sellers   = read_csv("olist_sellers_dataset.csv")
    payments  = read_csv("olist_order_payments_dataset.csv")
    reviews   = read_csv("olist_order_reviews_dataset.csv")
    cat_trans = read_csv("product_category_name_translation.csv")

    # --- Aggregate payments: pick dominant payment type per order ---
    window_pay = Window.partitionBy("order_id").orderBy(F.col("payment_value").desc())
    pay_agg = (
        payments
        .withColumn("_rn", F.row_number().over(window_pay))
        .filter(F.col("_rn") == 1)
        .select("order_id", "payment_type", "payment_value")
    )

    # --- Aggregate reviews: average score per order ---
    rev_agg = (
        reviews
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("review_score"))
    )

    # --- Enrich products with English category names ---
    products_en = products.join(
        cat_trans,
        on="product_category_name",
        how="left",
    ).select("product_id", "product_category_name", "product_category_name_english")

    # --- Build denormalized dataset (grain: one row per order item) ---
    df = (
        orders
        .join(items, on="order_id", how="left")
        .join(products_en, on="product_id", how="left")
        .join(
            customers.select(
                "customer_id", "customer_unique_id", "customer_city", "customer_state"
            ),
            on="customer_id",
            how="left",
        )
        .join(
            sellers.select("seller_id", "seller_city", "seller_state"),
            on="seller_id",
            how="left",
        )
        .join(pay_agg, on="order_id", how="left")
        .join(rev_agg, on="order_id", how="left")
    )

    # --- Cast, derive computed columns, and select final schema ---
    df_final = (
        df
        .withColumn("order_purchase_timestamp",
                    F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_delivered_customer_date",
                    F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date",
                    F.to_timestamp("order_estimated_delivery_date"))
        .withColumn("order_purchase_date",
                    F.to_date("order_purchase_timestamp"))
        .withColumn("order_purchase_year",
                    F.year("order_purchase_timestamp"))
        .withColumn("order_purchase_month",
                    F.month("order_purchase_timestamp"))
        .withColumn(
            "delivery_days",
            (
                F.unix_timestamp("order_delivered_customer_date")
                - F.unix_timestamp("order_purchase_timestamp")
            ) / 86400.0,
        )
        .withColumn(
            "estimated_delivery_days",
            (
                F.unix_timestamp("order_estimated_delivery_date")
                - F.unix_timestamp("order_purchase_timestamp")
            ) / 86400.0,
        )
        .withColumn(
            "is_on_time",
            F.when(
                F.col("order_delivered_customer_date").isNotNull()
                & (F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date")),
                True,
            ).otherwise(False),
        )
        .withColumn("price",          F.col("price").cast("double"))
        .withColumn("freight_value",  F.col("freight_value").cast("double"))
        .withColumn("total_item_value",
                    (F.col("price") + F.col("freight_value")).cast("double"))
        .withColumn("payment_value",  F.col("payment_value").cast("double"))
        .withColumn("review_score",   F.col("review_score").cast("double"))
        .withColumn("order_item_id",  F.col("order_item_id").cast("int"))
        .filter(F.col("order_purchase_date").isNotNull())
        .select(
            "order_id",
            "order_item_id",
            "order_status",
            "order_purchase_timestamp",
            "order_purchase_date",
            "order_purchase_year",
            "order_purchase_month",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "delivery_days",
            "estimated_delivery_days",
            "is_on_time",
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "seller_id",
            "seller_city",
            "seller_state",
            "price",
            "freight_value",
            "total_item_value",
            "payment_type",
            "payment_value",
            "review_score",
        )
    )

    row_count = df_final.count()
    print(f"[spark] Writing {row_count:,} rows to {output_path}")

    try:
        (
            df_final
            .repartition(8)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )
    except Py4JJavaError as exc:
        message = str(exc)
        if (
            (
                "NativeIO$Windows.access0" in message
                or "HADOOP_HOME and hadoop.home.dir are unset" in message
                or "getWinUtilsPath" in message
            )
            and not output_path.startswith("gs://")
        ):
            print("[spark] Windows Hadoop issue detected — using PyArrow fallback writer")
            write_with_pyarrow_fallback(df_final, output_path)
        else:
            raise

    print("[spark] Transform complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Olist E-Commerce PySpark transform")
    parser.add_argument("--gcs-bucket", default=None,
                        help="GCS bucket name (without gs:// prefix)")
    args = parser.parse_args()

    gcs_bucket = args.gcs_bucket or os.getenv("GCS_BUCKET", "")

    if gcs_bucket:
        raw_prefix = f"gs://{gcs_bucket}/raw"
        output_path = f"gs://{gcs_bucket}/processed/"
        enable_gcs = True
    else:
        base = Path(__file__).resolve().parent.parent
        raw_prefix = str(base / "data" / "raw")
        output_path = str(base / "data" / "processed")
        enable_gcs = False

    print(f"[spark] Raw prefix : {raw_prefix}")
    print(f"[spark] Output path: {output_path}")

    spark = get_spark(enable_gcs=enable_gcs)
    transform(spark, raw_prefix, output_path)
    spark.stop()


if __name__ == "__main__":
    main()
