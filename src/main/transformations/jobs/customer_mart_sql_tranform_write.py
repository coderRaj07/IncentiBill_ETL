from pyspark.sql.functions import col, lit, concat, sum, trunc
from pyspark.sql.window import Window
from src.main.write.database_write import DatabaseWriter

#calculation for customer mart
#find out the customer total purchase every month
#write the data into pgsql table
def customer_mart_calculation_table_write(final_customer_data_mart_df):
    # Convert to first day of month (e.g., 2024-05-01)
    df_with_month = final_customer_data_mart_df.withColumn(
        "sales_date_month", trunc(col("sales_date"), "MM")  # returns first day of the month as per schema expectation as sales_date_month DATE,
    )

    # Create window for customer and month
    window = Window.partitionBy("customer_id", "sales_date_month")

    # Aggregate total sales
    final_customer_data_mart = df_with_month.withColumn(
        "total_sales_every_month_by_each_customer",
        sum("total_cost").over(window)
    ).select(
        col("customer_id"),
        concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
        col("address"),
        col("phone_number"),
        col("sales_date_month"),
        col("total_sales_every_month_by_each_customer").alias("total_sales")
    ).distinct()

    # Write to PostgreSQL (assumes you have a writer class for this)
    DatabaseWriter().write_df_to_pgsql(final_customer_data_mart, "customers_data_mart")
