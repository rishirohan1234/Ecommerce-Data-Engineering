# Databricks notebook source
print("Welcome to Databricks!")

print("Starting E-Commerce Data Engineering Project")

# COMMAND ----------

print("Spark Version:", spark.version)

# COMMAND ----------

numbers = spark.range(1, 6)

display(numbers)

# COMMAND ----------

# Our Databricks Volume location

DATA_PATH = "/Volumes/workspace/default/ecommerce_raw/"

print("Dataset location:", DATA_PATH)

# COMMAND ----------

print(DATA_PATH)

# COMMAND ----------

display(dbutils.fs.ls(DATA_PATH))

# COMMAND ----------

import zipfile
import os

DATA_PATH = "/Volumes/workspace/default/ecommerce_raw/"

for file in os.listdir(DATA_PATH):
    if file.endswith(".zip"):
        zip_path = os.path.join(DATA_PATH, file)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DATA_PATH)

        print("Extracted:", file)

print("All ZIP files extracted successfully!")

# COMMAND ----------

display(dbutils.fs.ls(DATA_PATH))

# COMMAND ----------

customers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(DATA_PATH + "olist_customers_dataset.csv")

display(customers_df)

# COMMAND ----------

customer_count = customers_df.count()

print("Total customer records:", customer_count)

# COMMAND ----------

print(customers_df.columns)

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

# Load all datasets

customers_df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH + "olist_customers_dataset.csv")

orders_df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH + "olist_orders_dataset.csv")

order_items_df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH + "olist_order_items_dataset.csv")

products_df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH + "olist_products_dataset.csv")

payments_df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH + "olist_order_payments_dataset.csv")

reviews_df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH + "olist_order_reviews_dataset.csv")

sellers_df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH + "olist_sellers_dataset.csv")

print("All datasets loaded successfully!")

# COMMAND ----------

print("Customers:", customers_df.count())
print("Orders:", orders_df.count())
print("Order Items:", order_items_df.count())
print("Products:", products_df.count())
print("Payments:", payments_df.count())
print("Reviews:", reviews_df.count())
print("Sellers:", sellers_df.count())

# COMMAND ----------

print("Customers:", customers_df.count())
print("Orders:", orders_df.count())
print("Products:", products_df.count())
print("Payments:", payments_df.count())


# COMMAND ----------

print("Total records:", customers_df.count())

print("Duplicate records:",
      customers_df.count() - customers_df.dropDuplicates().count())

# COMMAND ----------

duplicate_customers = (
    customers_df
    .groupBy("customer_id")
    .count()
    .filter("count > 1")
)

display(duplicate_customers)

# COMMAND ----------

from pyspark.sql.functions import col, sum

missing_values = customers_df.select(
    *[
        sum(col(c).isNull().cast("int")).alias(c)
        for c in customers_df.columns
    ]
)

display(missing_values)

# COMMAND ----------

# Remove completely identical records

customers_clean = customers_df.dropDuplicates()

print("Original records:", customers_df.count())
print("Clean records:", customers_clean.count())

# COMMAND ----------

from pyspark.sql.functions import col, sum

missing_report = customers_clean.select(
    *[
        sum(col(c).isNull().cast("int")).alias(c)
        for c in customers_clean.columns
    ]
)

display(missing_report)

# COMMAND ----------

customers_clean = customers_clean.dropna(
    subset=["customer_id", "customer_unique_id"]
)

print("Records after removing invalid customer IDs:",
      customers_clean.count())

# COMMAND ----------

from pyspark.sql.functions import trim

customers_clean = (
    customers_clean
    .withColumn("customer_city", trim(col("customer_city")))
    .withColumn("customer_state", trim(col("customer_state")))
)

# COMMAND ----------

customers_clean = customers_clean.withColumnRenamed(
    "customer_city",
    "city"
)

customers_clean = customers_clean.withColumnRenamed(
    "customer_state",
    "state"
)

display(customers_clean)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

orders_clean = orders_df.dropDuplicates(["order_id"])

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column_name in date_columns:

    orders_clean = orders_clean.withColumn(
        column_name,
        to_timestamp(col(column_name))
    )

display(orders_clean)

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

order_items_clean = (
    order_items_df
    .dropDuplicates(["order_id", "order_item_id"])
    .filter(col("price") >= 0)
    .filter(col("freight_value") >= 0)
)

display(order_items_clean)

# COMMAND ----------

products_clean = (
    products_df
    .dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
)

display(products_clean)

# COMMAND ----------

payments_clean = (
    payments_df
    .dropDuplicates()
    .filter(col("payment_value") >= 0)
    .filter(col("payment_installments") > 0)
)

display(payments_clean)

# COMMAND ----------

from pyspark.sql.functions import col, expr

reviews_clean = (
    reviews_df
    .withColumn("review_score", expr("try_cast(review_score AS INT)"))
    .dropDuplicates(["review_id"])
    .filter(col("review_score").between(1, 5))
)

sellers_clean = (
    sellers_df
    .dropDuplicates(["seller_id"])
    .filter(col("seller_id").isNotNull())
)

print("Reviews:", reviews_clean.count())
print("Sellers:", sellers_clean.count())

# COMMAND ----------

reviews_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, expr

reviews_df = reviews_df.withColumn(
    "review_score",
    expr("try_cast(review_score AS INT)")
)

# COMMAND ----------

reviews_clean = (
    reviews_df
    .dropDuplicates(["review_id"])
    .filter(col("review_score").between(1, 5))
)

sellers_clean = (
    sellers_df
    .dropDuplicates(["seller_id"])
    .filter(col("seller_id").isNotNull())
)

print("Reviews:", reviews_clean.count())
print("Sellers:", sellers_clean.count())

# COMMAND ----------

reviews_clean.printSchema()
display(reviews_clean.limit(10))

# COMMAND ----------

reviews_clean = (
    reviews_df
    .dropDuplicates(["review_id"])
    .filter(col("review_score").between(1, 5))
)

sellers_clean = (
    sellers_df
    .dropDuplicates(["seller_id"])
    .filter(col("seller_id").isNotNull())
)

print("Reviews:", reviews_clean.count())
print("Sellers:", sellers_clean.count())

# COMMAND ----------

print("CUSTOMER DATA")
print("Before:", customers_df.count())
print("After:", customers_clean.count())

print("\nORDERS DATA")
print("Before:", orders_df.count())
print("After:", orders_clean.count())

print("\nORDER ITEMS DATA")
print("Before:", order_items_df.count())
print("After:", order_items_clean.count())

print("\nPRODUCT DATA")
print("Before:", products_df.count())
print("After:", products_clean.count())

# COMMAND ----------

cleaned_datasets = {
    "customers": customers_clean,
    "orders": orders_clean,
    "order_items": order_items_clean,
    "products": products_clean,
    "payments": payments_clean,
    "reviews": reviews_clean,
    "sellers": sellers_clean
}

for name, df in cleaned_datasets.items():

    print("\nDataset:", name)
    print("Total records:", df.count())
    print("Total columns:", len(df.columns))
    print("Duplicate records:", df.count() - df.dropDuplicates().count())

# COMMAND ----------

SILVER_PATH = "/Volumes/workspace/default/ecommerce_raw/silver/"

dbutils.fs.mkdirs(SILVER_PATH)

print("Silver folder created!")

# COMMAND ----------

# Save cleaned datasets in Delta format

for name, df in cleaned_datasets.items():

    output_path = SILVER_PATH + name

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)

    print(name, "saved successfully!")

# COMMAND ----------

customers_delta = spark.read \
    .format("delta") \
    .load(SILVER_PATH + "customers")

display(customers_delta)

# COMMAND ----------

GOLD_PATH = "/Volumes/workspace/default/ecommerce_raw/gold/"

dbutils.fs.mkdirs(GOLD_PATH)

print("Gold layer created successfully!")

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum, count, avg, round,
    month, year, date_format,
    countDistinct, datediff
)

# COMMAND ----------

order_revenue = (
    order_items_clean
    .groupBy("order_id")
    .agg(
        round(sum("price"), 2).alias("product_revenue"),
        round(sum("freight_value"), 2).alias("shipping_revenue")
    )
)

display(order_revenue)

# COMMAND ----------

total_revenue = order_revenue.agg(
    round(sum("product_revenue"), 2).alias("total_revenue")
)

display(total_revenue)

# COMMAND ----------

monthly_sales = (
    orders_clean
    .join(order_revenue, "order_id", "inner")
    .withColumn(
        "month",
        date_format("order_purchase_timestamp", "yyyy-MM")
    )
    .groupBy("month")
    .agg(
        round(sum("product_revenue"), 2).alias("monthly_revenue"),
        countDistinct("order_id").alias("total_orders")
    )
    .orderBy("month")
)

display(monthly_sales)

# COMMAND ----------

product_sales = (
    order_items_clean
    .groupBy("product_id")
    .agg(
        count("order_id").alias("total_units_sold"),
        round(sum("price"), 2).alias("total_revenue")
    )
    .join(products_clean, "product_id", "left")
    .orderBy(col("total_units_sold").desc())
)

top_products = product_sales.limit(10)

display(top_products)

# COMMAND ----------

city_sales = (
    orders_clean
    .join(order_revenue, "order_id", "inner")
    .join(
        customers_clean.select("customer_id", "city", "state"),
        "customer_id",
        "left"
    )
    .groupBy("city", "state")
    .agg(
        round(sum("product_revenue"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders")
    )
    .orderBy(col("total_revenue").desc())
)

top_cities = city_sales.limit(10)

display(top_cities)

# COMMAND ----------

customer_sales = (
    orders_clean
    .join(order_revenue, "order_id", "inner")
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("product_revenue"), 2).alias("total_spent"),
        round(avg("product_revenue"), 2).alias("average_order_value")
    )
    .orderBy(col("total_spent").desc())
)

display(customer_sales.limit(10))

# COMMAND ----------

payment_analysis = (
    payments_clean
    .groupBy("payment_type")
    .agg(
        count("order_id").alias("total_transactions"),
        round(sum("payment_value"), 2).alias("total_payment"),
        round(avg("payment_value"), 2).alias("average_payment")
    )
    .orderBy(col("total_payment").desc())
)

display(payment_analysis)

# COMMAND ----------

delivery_analysis = (
    orders_clean
    .filter(col("order_delivered_customer_date").isNotNull())
    .withColumn(
        "delivery_delay_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
    )
)

display(delivery_analysis.select(
    "order_id",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "delivery_delay_days"
))

# COMMAND ----------

delivery_summary = delivery_analysis.agg(
    count("*").alias("total_delivered"),
    sum(
        (col("delivery_delay_days") > 0).cast("int")
    ).alias("late_deliveries"),
    sum(
        (col("delivery_delay_days") <= 0).cast("int")
    ).alias("on_time_or_early")
)

display(delivery_summary)

# COMMAND ----------

gold_datasets = {
    "monthly_sales": monthly_sales,
    "product_sales": product_sales,
    "city_sales": city_sales,
    "customer_sales": customer_sales,
    "payment_analysis": payment_analysis,
    "delivery_analysis": delivery_analysis
}

for name, df in gold_datasets.items():

    output_path = GOLD_PATH + name

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)

    print(name, "saved successfully!")

# COMMAND ----------

monthly_sales.createOrReplaceTempView("monthly_sales")
product_sales.createOrReplaceTempView("product_sales")
city_sales.createOrReplaceTempView("city_sales")
customer_sales.createOrReplaceTempView("customer_sales")
payment_analysis.createOrReplaceTempView("payment_analysis")
delivery_analysis.createOrReplaceTempView("delivery_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     month,
# MAGIC     monthly_revenue,
# MAGIC     total_orders
# MAGIC FROM monthly_sales
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     total_units_sold,
# MAGIC     total_revenue
# MAGIC FROM product_sales
# MAGIC ORDER BY total_units_sold DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     city,
# MAGIC     state,
# MAGIC     total_revenue
# MAGIC FROM city_sales
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     total_orders,
# MAGIC     total_spent
# MAGIC FROM customer_sales
# MAGIC WHERE total_spent > 1000
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     payment_type,
# MAGIC     total_transactions,
# MAGIC     total_payment,
# MAGIC     average_payment
# MAGIC FROM payment_analysis
# MAGIC ORDER BY total_payment DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(
# MAGIC         CASE WHEN delivery_delay_days > 0
# MAGIC         THEN 1 ELSE 0 END
# MAGIC     ) AS late_orders
# MAGIC FROM delivery_analysis;

# COMMAND ----------

display(monthly_sales)

# COMMAND ----------

display(top_cities)

# COMMAND ----------

monthly_sales.createOrReplaceTempView("monthly_sales")
product_sales.createOrReplaceTempView("product_sales")
city_sales.createOrReplaceTempView("city_sales")
customer_sales.createOrReplaceTempView("customer_sales")
payment_analysis.createOrReplaceTempView("payment_analysis")
delivery_analysis.createOrReplaceTempView("delivery_analysis")

print("All SQL views created successfully!")

# COMMAND ----------

print("Orders count:", orders_clean.count())

orders_clean.show(5, truncate=False)

# COMMAND ----------

orders_clean.createOrReplaceTempView("orders_clean")
customers_clean.createOrReplaceTempView("customers_clean")

print("Temporary views registered successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.city,
# MAGIC     o.order_id
# MAGIC FROM customers_clean c
# MAGIC INNER JOIN orders_clean o
# MAGIC     ON c.customer_id = o.customer_id;

# COMMAND ----------

customers_clean.createOrReplaceTempView("customers_clean")

orders_clean.createOrReplaceTempView("orders_clean")

print("SQL views created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.city,
# MAGIC     o.order_id
# MAGIC FROM customers_clean c
# MAGIC INNER JOIN orders_clean o
# MAGIC     ON c.customer_id = o.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.city,
# MAGIC     o.order_id
# MAGIC FROM customers_clean c
# MAGIC INNER JOIN orders_clean o
# MAGIC     ON c.customer_id = o.customer_id;

# COMMAND ----------

customers_clean.createOrReplaceTempView("customers_clean")
orders_clean.createOrReplaceTempView("orders_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH high_value_customers AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         total_spent
# MAGIC     FROM customer_sales
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM high_value_customers
# MAGIC WHERE total_spent > 1000
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     total_spent,
# MAGIC
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY total_spent DESC
# MAGIC     ) AS customer_rank
# MAGIC
# MAGIC FROM customer_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH ranked_customers AS (
# MAGIC     SELECT
# MAGIC         c.customer_id,
# MAGIC         u.city,
# MAGIC         c.total_spent,
# MAGIC
# MAGIC         DENSE_RANK() OVER (
# MAGIC             PARTITION BY u.city
# MAGIC             ORDER BY c.total_spent DESC
# MAGIC         ) AS customer_rank
# MAGIC
# MAGIC     FROM customer_sales c
# MAGIC     JOIN customers_clean u
# MAGIC         ON c.customer_id = u.customer_id
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ranked_customers
# MAGIC WHERE customer_rank <= 3
# MAGIC ORDER BY city, customer_rank;

# COMMAND ----------

display(orders_clean)

# COMMAND ----------

INCREMENTAL_PATH = "/Volumes/workspace/default/ecommerce_raw/silver/orders_incremental/"

print("Incremental location:", INCREMENTAL_PATH)

# COMMAND ----------

from pyspark.sql.functions import col

initial_orders = orders_clean.filter(
    col("order_purchase_timestamp") < "2017-06-01"
)

print("Initial historical orders:", initial_orders.count())

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_timestamp

incoming_orders = orders_clean.filter(
    (col("order_purchase_timestamp") >= to_timestamp(lit("2017-06-01 00:00:00"))) &
    (col("order_purchase_timestamp") < to_timestamp(lit("2017-07-01 00:00:00")))
)

print("Incoming orders:", incoming_orders.count())

# COMMAND ----------

from delta.tables import DeltaTable

if not DeltaTable.isDeltaTable(spark, INCREMENTAL_PATH):

    initial_orders.write \
        .format("delta") \
        .mode("overwrite") \
        .save(INCREMENTAL_PATH)

    print("Initial data loaded successfully!")

else:
    print("Delta table already exists. Skipping initial load.")

# COMMAND ----------

existing_orders = spark.read \
    .format("delta") \
    .load(INCREMENTAL_PATH)

print("Existing order count:", existing_orders.count())

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def process_incremental_batch(batch_df):

    # Remove invalid order IDs
    valid_orders = batch_df.filter(
        col("order_id").isNotNull()
    )

    # Remove duplicate order IDs within the batch
    valid_orders = valid_orders.dropDuplicates(["order_id"])

    # Access existing Delta table
    delta_table = DeltaTable.forPath(
        spark,
        INCREMENTAL_PATH
    )

    # Insert new records and update existing records
    delta_table.alias("target").merge(
        valid_orders.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print("Incremental batch processed successfully!")
    print("Incoming records:", batch_df.count())
    print("Valid records:", valid_orders.count())

    print(
        "Total records after processing:",
        spark.read.format("delta")
        .load(INCREMENTAL_PATH).count()
    )

# COMMAND ----------

process_incremental_batch(incoming_orders)

# COMMAND ----------

process_incremental_batch(incoming_orders)

# COMMAND ----------

final_orders = spark.read \
    .format("delta") \
    .load(INCREMENTAL_PATH)

print("Final order count:", final_orders.count())

print(
    "Unique order IDs:",
    final_orders.select("order_id").distinct().count()
)

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row

def log_pipeline_run(batch_name, received, processed, status):

    log = [
        Row(
            batch_name=batch_name,
            records_received=received,
            records_processed=processed,
            status=status,
            execution_time=datetime.now()
        )
    ]

    log_df = spark.createDataFrame(log)

    log_df.write \
        .format("delta") \
        .mode("append") \
        .save(
            "/Volumes/workspace/default/ecommerce_raw/silver/pipeline_logs/"
        )

    print("Pipeline execution logged!")

# COMMAND ----------

log_pipeline_run(
    "June_2017_Batch",
    incoming_orders.count(),
    incoming_orders.filter(
        col("order_id").isNotNull()
    ).dropDuplicates(["order_id"]).count(),
    "SUCCESS"
)

# COMMAND ----------

logs_df = spark.read \
    .format("delta") \
    .load(
        "/Volumes/workspace/default/ecommerce_raw/silver/pipeline_logs/"
    )

display(logs_df)

# COMMAND ----------

reviews_df.printSchema()
orders_df.printSchema()
order_items_df.printSchema()
payments_df.printSchema()

# COMMAND ----------

display(orders_df.limit(5))
display(reviews_df.limit(5))

# COMMAND ----------

print("Order date type:",
      orders_df.schema["order_purchase_timestamp"].dataType)

print("Review date type:",
      reviews_df.schema["review_creation_date"].dataType)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

reviews_df = reviews_df.withColumn(
    "review_creation_date",
    to_timestamp(col("review_creation_date"))
)

reviews_df = reviews_df.withColumn(
    "review_answer_timestamp",
    to_timestamp(col("review_answer_timestamp"))
)

reviews_df.printSchema()

# COMMAND ----------

ORDERS_PATH = "/Volumes/workspace/default/ecommerce_raw/olist_orders_dataset.csv"

display(dbutils.fs.ls(
    "/Volumes/workspace/default/ecommerce_raw/"
))

# COMMAND ----------

ORDERS_PATH = "/Volumes/workspace/default/ecommerce_raw/olist_orders_dataset.csv"

print(dbutils.fs.head(ORDERS_PATH, 500))

# COMMAND ----------

test_orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(ORDERS_PATH)

display(test_orders.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ORDERS_PATH)
)

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in date_columns:
    orders_df = orders_df.withColumn(
        column,
        to_timestamp(col(column), "yyyy-MM-dd HH:mm:ss")
    )

orders_df.printSchema()

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_timestamp

orders_clean = orders_df.dropDuplicates(["order_id"])

incoming_orders = orders_clean.filter(
    (col("order_purchase_timestamp") >=
     to_timestamp(lit("2017-06-01 00:00:00"))) &
    (col("order_purchase_timestamp") <
     to_timestamp(lit("2017-07-01 00:00:00")))
)

print("Incoming orders:", incoming_orders.count())

display(incoming_orders.limit(10))

# COMMAND ----------

print("INITIAL ORDERS SCHEMA")
initial_orders.printSchema()

print("INCOMING ORDERS SCHEMA")
incoming_orders.printSchema()

# COMMAND ----------

try:
    existing_delta_df = (
        spark.read
        .format("delta")
        .load(INCREMENTAL_PATH)
    )

    print("EXISTING DELTA TABLE SCHEMA")
    existing_delta_df.printSchema()

except Exception as e:
    print("Error reading existing Delta table:")
    print(str(e)[:1500])

# COMMAND ----------

print("Initial orders:", initial_orders.count())
print("Incoming orders:", incoming_orders.count())
print("Existing orders:", existing_orders.count())

print("Initial data types:")
print(initial_orders.dtypes)

print("Incoming data types:")
print(incoming_orders.dtypes)

# COMMAND ----------

ORDERS_PATH = "/Volumes/workspace/default/ecommerce_raw/olist_orders_dataset.csv"

fresh_orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ORDERS_PATH)
)

print("Schema loaded:")
fresh_orders.printSchema()

# COMMAND ----------

print("Total orders:", fresh_orders.count())

# COMMAND ----------

orders_df = fresh_orders

from pyspark.sql.functions import col, to_timestamp

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for c in date_columns:
    orders_df = orders_df.withColumn(
        c,
        to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss")
    )

orders_clean = orders_df.dropDuplicates(["order_id"])

print("Clean orders:", orders_clean.count())

# COMMAND ----------

from pyspark.sql.functions import lit

incoming_orders = orders_clean.filter(
    (col("order_purchase_timestamp") >=
     to_timestamp(lit("2017-06-01 00:00:00"))) &
    (col("order_purchase_timestamp") <
     to_timestamp(lit("2017-07-01 00:00:00")))
)

print("Incoming orders:", incoming_orders.count())

# COMMAND ----------

existing_orders = (
    spark.read
    .format("delta")
    .load(INCREMENTAL_PATH)
)

print("Existing orders:", existing_orders.count())

print(
    "Matching incoming orders:",
    existing_orders.join(
        incoming_orders.select("order_id"),
        on="order_id",
        how="inner"
    ).count()
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

valid_orders = (
    incoming_orders
    .filter(col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

delta_table = DeltaTable.forPath(
    spark,
    INCREMENTAL_PATH
)

delta_table.alias("target").merge(
    valid_orders.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Incremental batch processed successfully!")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

valid_orders = (
    incoming_orders
    .filter(col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

delta_table = DeltaTable.forPath(
    spark,
    INCREMENTAL_PATH
)

delta_table.alias("target").merge(
    valid_orders.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Incremental batch processed successfully!")

# COMMAND ----------

final_orders = (
    spark.read
    .format("delta")
    .load(INCREMENTAL_PATH)
)

print("Final order count:", final_orders.count())

print(
    "Unique order IDs:",
    final_orders.select("order_id").distinct().count()
)

# COMMAND ----------

orders_df = spark.read \
    .format("csv")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

ORDERS_PATH = "/Volumes/workspace/default/ecommerce_raw/olist_orders_dataset.csv"

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ORDERS_PATH)
)

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for c in date_columns:
    orders_df = orders_df.withColumn(
        c,
        to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss")
    )

print("Orders loaded successfully!")
orders_df.printSchema()

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

valid_orders = (
    incoming_orders
    .filter(col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

delta_table = DeltaTable.forPath(spark, INCREMENTAL_PATH)

delta_table.alias("target").merge(
    valid_orders.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Incremental batch processed successfully!")

# COMMAND ----------

final_orders = (
    spark.read
    .format("delta")
    .load(INCREMENTAL_PATH)
)

print("Final order count:", final_orders.count())

print(
    "Unique order IDs:",
    final_orders.select("order_id").distinct().count()
)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, INCREMENTAL_PATH)

display(delta_table.history())

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, INCREMENTAL_PATH)

display(delta_table.history())

# COMMAND ----------

GOLD_PATH = "/Volumes/workspace/default/ecommerce_raw/gold/"

monthly_sales.toPandas().to_csv(
    GOLD_PATH + "monthly_sales.csv", index=False
)

product_sales.toPandas().to_csv(
    GOLD_PATH + "product_sales.csv", index=False
)

city_sales.toPandas().to_csv(
    GOLD_PATH + "city_sales.csv", index=False
)

customer_sales.toPandas().to_csv(
    GOLD_PATH + "customer_sales.csv", index=False
)

payment_analysis.toPandas().to_csv(
    GOLD_PATH + "payment_analysis.csv", index=False
)

delivery_analysis.toPandas().to_csv(
    GOLD_PATH + "delivery_analysis.csv", index=False
)

print("Gold datasets exported successfully!")

# COMMAND ----------

BASE_PATH = "/Volumes/workspace/default/ecommerce_raw/"

datasets = [
    "monthly_sales",
    "product_sales",
    "city_sales",
    "customer_sales",
    "payment_analysis",
    "delivery_analysis"
]

for dataset in datasets:

    df = spark.read.format("delta").load(
        BASE_PATH + "gold/" + dataset
    )

    output_path = BASE_PATH + "gold_exports/" + dataset

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("escape", '"') \
        .csv(output_path)

    print(dataset, "exported successfully!")

print("All six Gold datasets exported!")