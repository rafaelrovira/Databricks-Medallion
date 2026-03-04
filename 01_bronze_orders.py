# Databricks notebook source

#Começo do medalhão, ler a raw e criar a bronze

df_raw = spark.table("raw_orders")

df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_orders")