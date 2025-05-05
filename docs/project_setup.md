# 📊 BillWise – focuses on incentive logic based on billing.
## Data Processing Pipeline

This project is built to automate the flow of downloading sales data from S3, validating and transforming it using PySpark, and updating a PostgreSQL database. It also detects data inconsistencies and routes them appropriately.

---

## 🚀 Project Setup & Execution Guide

### 1. Clone the Repository

```bash
git clone https://github.com/coderRaj07/Billwise_DE_Project
cd Billwise_DE_Project
```

---

### 2. Set Environment Variables

Make sure you export the required environment variables as per your environment (`dev`, `qa`, or `prod`):

```bash
export ENV=dev  # or qa/prod as needed
```

Then, source the config:

```bash
source resources/$ENV/config.py
```

> Alternatively, you can manually load variables defined in `resources/dev/config.py`.

---

### 3. Install Requirements

Use the pip requirements file from the corresponding environment:

```bash
pip install -r resources/dev/requirement.txt
```

---

### 4. Run Setup Scripts

Before running the main pipeline, generate your tables and dummy data:

```bash
# Step 1: Generate tables
python resources/scripts/generate_tables_script.py

# Step 2: Populate dummy data
python resources/scripts/generate_dummy_data_script.py
```

These scripts will output sample CSVs in a folder named `sales_data_for_s3/`:

* `extra_column_csv_generated_sales_data_<date>.csv`
* `generated_csv_sales_data.csv`
* `generated_datewise_sales_data_<date>.csv`
* `less_column_csv_generated_sales_data_<date>.csv`

---

### 5. Run the Pipeline

The pipeline fetches files from S3, processes them with PySpark, and updates the database accordingly.

* CSVs with **fewer columns** are sent to an S3 error folder.
* CSVs with **extra columns** get extra fields added for lookups before proceeding.
* Valid records update the database.
* High billers are rewarded through further logic (coming soon).

> Main transformation jobs are located in:
> `src/main/transformations/jobs/`

---

## 📁 Directory Summary

```
resources/dev/config.py       # Environment-specific variables
resources/scripts/            # Table + dummy data generation scripts
src/main/                     # Core ETL modules
src/test/                     # Spark & data generation tests
```

---

## ✅ Next Steps

* Ensure you have AWS credentials set up and properly encrypted (for S3 operations).
* PostgreSQL should be running and accessible with the credentials from `config.py`.
* PySpark should be properly installed and working with your system's Hadoop version.

---