# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
display(
    spark.sql("SELECT * FROM workspace.default.walmart_dataset")
)

# COMMAND ----------

