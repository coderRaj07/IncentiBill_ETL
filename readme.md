
# 📊 IncentiBill – Incentive-Driven Billing and Reward System

## 🚨 Pre-Project Setup

Before starting this project, ensure that the following tools are installed on your system:

* **PySpark**
* **Hadoop**
* **Java**

If they are not installed, follow the detailed guide for Ubuntu here:
[Install Java, PySpark, and Hadoop on Ubuntu for Data Engineering Projects](https://coderraj07.medium.com/install-java-pyspark-and-hadoop-on-ubuntu-for-data-engineering-projects-0f82fd86fa44)

---

## 🖥️ Virtual Environment Setup

If you're using **VSCode**, it’s recommended to create a virtual environment. Follow this guide for setting up your Python environments:
[Managing Python Environments: The Right Way (Conda vs. venv)](https://towardsdev.com/managing-python-environments-the-right-way-conda-vs-venv-1691162a7016)

Once the environment is set up, install the project dependencies:

```bash
pip install -r resources/dev/requirement.txt
```

---

## 🔧 Project Setup & Execution Guide

### 1. Clone the Repository

```bash
git clone https://github.com/coderRaj07/IncentiBill_ETL
cd IncentiBill_ETL
```

---

### 2. Set Environment Variables

Refer to .env.example and create the .env file in the root directory (IncentiBill_ETL)

---

### 3. Install Requirements

Use the pip requirements file from the corresponding environment:

```bash
pip install -r resources/dev/requirement.txt
```

---

### 4. Create Required Folders for S3

Before running the main pipeline, create the necessary folders in s3 using the `values` defined in `resources/dev/config.py`:

```python
bucket_name = "de-project-testing-aws"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
s3_sales_partitioned_datamart_directory = "sales_partitioned_data_mart/"
```

---

### 5. Run Setup Scripts

Generate your tables and dummy data:

```bash
# Step 1: Generate tables
python3 -m resources.scripts.generate_tables_script

# Step 2: Populate dummy data
python3 -m resources.scripts.generate_dummy_data_script

# Step 3: Upload dummy data to S3
python3 -m src.test.sales_data_upload_s3
```

These scripts will output sample CSVs in a folder named `sales_data_for_s3/`:

* `extra_column_csv_generated_sales_data_<date>.csv`
* `generated_csv_sales_data.csv`
* `generated_datewise_sales_data_<date>.csv`
* `less_column_csv_generated_sales_data_<date>.csv`

---

### 6. Run the Pipeline

The pipeline fetches files from **S3**, processes them with **PySpark**, and updates the database accordingly:

1. **CSV files with fewer columns** are moved to the **S3 error folder** (`sales_data_error/`).
2. **CSV files with extra columns** get extra fields added for lookups before processing.
3. **Valid records** update the database.
4. **High billers** are rewarded through further logic.

> Main transformation jobs are located in:
> `src/main/transformations/jobs/`

```bash
# Run the pipeline from the root directory i.e, `IncentiBill_ETL`
python3 -m src.main.transformations.jobs.main
```
---

## 🔄 Project Flow (In Short)

1. **Source Directory (S3)**: Files from `config.s3_source_directory` are verified for mandatory columns.

   * If **less than mandatory columns** are present or **non-CSV files** are found, they are moved to `config.s3_error_directory`, and the status is marked as "A" (processing in the database).
2. **Datamart Creation**: Valid data is used to create **Sales** and **Customer** datamarts and is uploaded to the respective S3 directories:

   * `config.s3_sales_datamart_directory`
   * `config.s3_customer_datamart_directory`
3. **Sales Partitioning**: Sales data is partitioned and pushed to `config.s3_sales_partitioned_datamart_directory`.
4. **Incentivizing Top Performers**: Apply transformation logic to incentivize top performers.
5. **Processed Data**: Finally, all processed files are moved from `config.s3_source_directory` to `config.s3_processed_directory`, and the status is marked as "I" (Inactive means completed in the database).

---

## 📁 Directory Summary

```
resources/dev/config.py       # Environment-specific variables
resources/scripts/            # Table + dummy data generation scripts
src/main/                     # Core ETL modules
src/test/                     # Spark & data generation tests
```

---

## ⚙️ Tech Stack

* **Programming Language**: Python3
* **Framework**: PySpark
* **Database**: PostgreSQL
* **Data Storage**: AWS S3
* **Cloud Service**: AWS (for storage and file processing)

---

## ✅ Next Steps

* Ensure you have **AWS credentials** set up and properly encrypted for S3 operations.
* **PostgreSQL** should be running and accessible with the credentials from `config.py`.
* **PySpark** should be properly installed and compatible with your system's Hadoop version.

---
