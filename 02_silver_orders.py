# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------
# 1. Leitura da camada Bronze (tabela Delta)
# ------------------------------------------

df = spark.table("bronze_orders")

# ------------------------------------------
# 2. Padronização e reafirmação do schema
# ------------------------------------------

df = (
    df
    .withColumn("id", F.col("id").cast("long"))
    .withColumn("order_date", F.col("order_date").cast("date"))
    .withColumn("ship_mode", F.lower(F.trim(F.col("ship_mode"))))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("sales", F.col("sales").cast("double"))
)

# ------------------------------------------
# 3. Regras técnicas de qualidade de dados
# ------------------------------------------

df = (
    df
    # chave primária não pode ser nula
    .filter(F.col("id").isNotNull())
    
    # data do pedido não pode ser nula
    .filter(F.col("order_date").isNotNull())
    
    # valor da venda não pode ser nulo ou negativo
    .filter(F.col("sales").isNotNull())
    .filter(F.col("sales") >= 0)
    
    # data do pedido não pode ser futura
    .filter(F.col("order_date") <= F.current_date())
)

# ------------------------------------------
# 4. Deduplicação determinística
# ------------------------------------------

window = Window.partitionBy("id").orderBy("order_date")

df = (
    df
    .withColumn("row_number", F.row_number().over(window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# ------------------------------------------
# 5. Colunas técnicas (auditoria)
# ------------------------------------------

df = (
    df
    .withColumn("processed_at", F.current_timestamp())
    .withColumn("source_table", F.lit("bronze_orders"))
)

# ------------------------------------------
# 6. Escrita da camada Silver
# ------------------------------------------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")