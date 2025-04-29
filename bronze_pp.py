# bronze_pp.py
# ETL: RAW CSV -> BRONZE Parquet sin duplicados
# Solo inserta nuevas transacciones no existentes en Bronze.

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Config
BUCKET          = "tapi-challenge"                   # Tu bucket
INPUT_PATH      = f"s3://{BUCKET}/raw/payments/"
BRONZE_PATH     = f"s3://{BUCKET}/processed/payments/"

# Iniciar Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 1. Leer nuevos datos de RAW
df_new = (spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(INPUT_PATH)
)

# Filtrar recargas
df_new = (df_new
    .filter(F.col("company_type") == "RECHARGE")
    .withColumn("created_at_ts", F.to_timestamp("created_at"))
    .filter(F.col("created_at_ts").isNotNull())
)

# 2. Leer datos existentes de BRONZE (si existen)
try:
    df_existing = spark.read.parquet(BRONZE_PATH)
    df_existing_ids = df_existing.select("operation_id").distinct()
    print("✅ Bronze existente encontrado.")
except Exception as e:
    df_existing_ids = None
    print("ℹ️  Bronze vacío o no encontrado: cargaremos todo lo nuevo.")

# 3. Eliminar duplicados (comparando operation_id)
if df_existing_ids:
    df_new = df_new.join(df_existing_ids, on="operation_id", how="left_anti")
    print("✅ Filtrados duplicados ya presentes en Bronze.")

# 4. Agregar columnas de partición
df_new = (df_new
    .withColumn("year",  F.year("created_at_ts"))
    .withColumn("month", F.month("created_at_ts"))
    .drop("created_at_ts")
)

# 5. Grabar sólo las transacciones nuevas
(df_new.write
    .mode("append")
    .partitionBy("year", "month")
    .format("parquet")
    .save(BRONZE_PATH)
)

print("🚀  bronze_pp: carga sin duplicados completada.")
