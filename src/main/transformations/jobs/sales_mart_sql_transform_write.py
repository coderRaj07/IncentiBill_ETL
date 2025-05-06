from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter

# Calculate total sales per sales person every month and write to PostgreSQL
def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
    # Extract month and year (e.g., '2024-10')
    df_with_month = final_sales_team_data_mart_df.withColumn(
        "sales_month", date_format(col("sales_date"), "yyyy-MM")  # Format as 'YYYY-MM' sales_month VARCHAR(10), as defined in schema
    )

    # Window to calculate total sales per salesperson per store per month
    window_spec = Window.partitionBy("store_id", "sales_person_id", "sales_month")

    sales_aggregated_df = (
        df_with_month
        .withColumn("total_sales_every_month", sum(col("total_cost")).over(window_spec))
        .withColumn("full_name", concat(col("sales_person_first_name"), lit(" "), col("sales_person_last_name")))
        .select(
            "store_id",
            "sales_person_id",
            "full_name",
            "sales_month",
            "total_sales_every_month"
        )
        .distinct()
    )

    # Window to rank salespeople by total sales within each store and month
    rank_window = Window.partitionBy("store_id", "sales_month").orderBy(
        col("total_sales_every_month").desc()
    )

    final_sales_team_data_mart_table = (
        sales_aggregated_df
        .withColumn("rnk", rank().over(rank_window))
        .withColumn("incentive", when(col("rnk") == 1, col("total_sales_every_month") * 0.01).otherwise(lit(0.0)))
        .withColumn("incentive", round(col("incentive"), 2))
        .withColumnRenamed("total_sales_every_month", "total_sales")
        .select(
            "store_id",
            "sales_person_id",
            "full_name",
            "sales_month",
            "total_sales",
            "incentive"
        )
    )

    # Write to PostgreSQL
    DatabaseWriter().write_df_to_pgsql(final_sales_team_data_mart_table, config.sales_team_data_mart_table)
