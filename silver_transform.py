# silver_transform.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, year, month, round as sround,
    monotonically_increasing_id
)

# ----------------- 0. Spark session -----------------
spark = (SparkSession.builder
         .appName("tapi_silver_transform")
         .getOrCreate())

# Paths hardcodeados
bucket_bronze = "s3://tapi-challenge/processed"
bucket_silver = "s3://tapi-challenge/silver"

# ----------------- 1. Lectura de Bronze -----------------
payments = (spark.read.parquet(f"{bucket_bronze}/payments/"))

providers_commission = (spark.read.parquet(f"{bucket_bronze}/providers-comission/"))

clients_revenue_share = (spark.read.parquet(f"{bucket_bronze}/clients-revenue-share/"))

# ----------------- 2. Limpieza de pagos -----------------
payments = (payments
  .filter((col("status") == "confirmed") &
          (col("company_type") == "RECHARGE"))
  .withColumn("amount", col("amount").cast("double"))
  .withColumn("amount_usd", col("amount_usd").cast("double"))
  .withColumn("created_at", col("created_at").cast("timestamp"))
)

# ----------------- 3. Limpieza de commissions -----------------
providers_commission = (providers_commission
  .filter((trim(col("commission_type")) == "amount") &
          (col("company_type") == "RECHARGE"))
  .select("external_provider_id", "company_code", "country_code", "tapi_commission")
  .dropDuplicates()
  .withColumnRenamed("tapi_commission", "provider_commission_rate")
)

clients_revenue_share = (clients_revenue_share
  .filter((trim(col("commission_type")) == "revenue_share") &
          (col("company_type") == "RECHARGE"))
  .select("client", "country_code", "client_commission")
  .dropDuplicates()
  .withColumnRenamed("client_commission", "client_revenue_share_rate")
)

# ----------------- 4. Enriquecimiento: traer tasas reales -----------------
payments_with_provider_rate = payments.join(
    providers_commission,
    ["external_provider_id", "company_code", "country_code"],
    "left"
)

payments_enriched = payments_with_provider_rate.join(
    clients_revenue_share,
    ["client", "country_code"],
    "left"
)

# ----------------- 5. Construcción de dim_client -----------------
dim_client = (payments_enriched.select("client")
              .dropDuplicates()
              .withColumn("client_sk", monotonically_increasing_id())
              .select("client_sk", "client"))

dim_client.write.mode("overwrite").parquet(f"{bucket_silver}/dim_client")

# ----------------- 6. Construcción de dim_provider -----------------
dim_provider = (payments_enriched.select("external_provider_id", "company_code", "company_name")
                .dropDuplicates()
                .withColumn("provider_sk", monotonically_increasing_id())
                .select("provider_sk", "external_provider_id", "company_code", "company_name"))

dim_provider.write.mode("overwrite").parquet(f"{bucket_silver}/dim_provider")

# ----------------- 7. Armado de fact_payments -----------------
payments_with_provider = payments_enriched.join(
    dim_provider,
    ["external_provider_id", "company_code", "company_name"],
    "left"
)

payments_with_keys = payments_with_provider.join(
    dim_client,
    ["client"],
    "left"
)

fact_payments = (payments_with_keys
  .withColumn("tapi_commission_amount", sround(col("amount") * col("provider_commission_rate"), 4))
  .withColumn("client_revenue_share_amount", sround(col("tapi_commission_amount") * col("client_revenue_share_rate"), 4))
  .withColumn("tapi_net_revenue", col("tapi_commission_amount") - col("client_revenue_share_amount"))
  .withColumn("year", year("created_at"))
  .withColumn("month", month("created_at"))
)

fact_payments = fact_payments.select(
    "operation_id",
    "client_sk",
    "provider_sk",
    "amount",
    "amount_usd",
    "provider_commission_rate",
    "client_revenue_share_rate",
    "tapi_commission_amount",
    "client_revenue_share_amount",
    "tapi_net_revenue",
    "created_at",
    "year",
    "month"
)

fact_payments.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{bucket_silver}/fact_payments")

# ----------------- 8. Cerrar Spark session -----------------
spark.stop()
