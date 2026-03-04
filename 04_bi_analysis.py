# Databricks notebook source
# MAGIC %sql
# MAGIC -- Faturamento por dia vindo da Gold
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_sales,
# MAGIC   total_orders
# MAGIC FROM gold_revenue_by_day
# MAGIC ORDER BY order_date;
# MAGIC
# MAGIC -- By ship mode
# MAGIC
# MAGIC --SELECT
# MAGIC --  ship_mode,
# MAGIC --  total_sales,
# MAGIC --  total_orders
# MAGIC --FROM gold_revenue_by_ship_mode
# MAGIC --ORDER BY total_sales DESC;
# MAGIC