# Databricks notebook source
from pyspark.sql import functions as F

# ------------------------------------------
# 1. Leitura da camada Silver
# ------------------------------------------
df = spark.table("silver_orders")

# ------------------------------------------
# 2. Métrica 1: Faturamento por data
# ------------------------------------------

# Soma das vendas por dia
df_revenue_by_day = (
    df
    .groupBy("order_date")
    .agg(
        F.sum("sales").alias("total_sales"),
        F.countDistinct("id").alias("total_orders")
    )
)

# ------------------------------------------
# 3. Escrita da tabela Gold - por data
# ------------------------------------------

# Tabela pronta para análises temporais
df_revenue_by_day.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_revenue_by_day")

# ------------------------------------------
# 4. Métrica 2: Faturamento por ship_mode
# ------------------------------------------
# Análise de performance por tipo de envio
df_revenue_by_ship_mode = (
    df
    .groupBy("ship_mode")
    .agg(
        F.sum("sales").alias("total_sales"),
        F.countDistinct("id").alias("total_orders")
    )
)

# ------------------------------------------
# 5. Escrita da tabela Gold - por ship_mode
# ------------------------------------------
# Útil para análises logísticas e BI
df_revenue_by_ship_mode.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_revenue_by_ship_mode")