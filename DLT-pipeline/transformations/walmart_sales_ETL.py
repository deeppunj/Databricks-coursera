import dlt
from pyspark.sql.functions import col, to_date, sum, avg, desc

#-------
# Brozen layer: raw data ingestion 
#-------
# we ingest data directly from the Unity catalog table/location.

@dlt.table(
    name = "walmart_bronze",
    comment = "Bronze table: Raw walmart data directly ingested from the workspace."
)

def walmart_raw():
    # reading as a stream is the best practice for DLT to process new data incrementally
    return spark.readStream.table("workspace.default.walmart_dataset")


#------
# silver layer: cleansed and prepared data
#-------
# here we apply data quality expectations, fix data types, and drop raw metadata columns.

@dlt.table(
    name = "walmart_silver",
    comment = "Silver table: cleansed Walmart data with proper data types and quality checks."
)

# DLT expectations (data quality checks)
@dlt.expect_or_drop("valid_sales", "Weekly_Sales>0") # Drop rows with negative /zero sales
@dlt.expect("valid_store_id", "Store IS NOT NULL") # Flag rows missing a Store ID

def walmart_prepared():
    return(
        dlt.read_stream("walmart_bronze")
        # the data in the sample is YYYY-MM-DD format, so we cast it to an actual DateType
        .withColumn('Date', to_date(col("Date"), "dd/MM/yy"))
        # selecting only the business-relevant columns, leaving behind raw metadata like _bronze_id
        .select(
            "Store",
            "Date",
            "Weekly_Sales",
            "Holiday_Flag",
            "Temperature",
            "Fuel_Price",
            "CPI",
            "Unemployment",
            "temp_cat"
        )
    )

#------
# Gold layer: aggregated insights for Business analytics
#-------
# Creating a summarized view that a BI tool or business analyst would use. 
@dlt.table(
    name = "walmart_gold_store_performance",
    comment = "Gold table: Aggregated total sales and average temperature per store by weather category."
)

def store_sales_summary():
    # Gold tables often read as statis batches rather than streams to compute full aggregations
    return(
        dlt.read("walmart_silver")
        .groupBy("Store", "temp_cat")
        .agg(
            sum("Weekly_Sales").alias("Total_Sales"),
            avg("Temperature").alias("Avg_Temperature")
        )
        # sorting the output to see the highest-performing stores at the top
        .sort(desc("Total_Sales"))
    )