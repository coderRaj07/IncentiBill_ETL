import subprocess

def run_all_tests():
    commands = [
        "python3 -m src.test.extra_column_csv_generated_data",
        "python3 -m src.test.generate_csv_data",
        "python3 -m src.test.generate_datewise_sales_data",
        "python3 -m src.test.less_column_csv_generated_data",
        "python3 -m src.test.generate_customer_table_data",
    ]

    for cmd in commands:
        print(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            print(f"Command failed: {cmd}")
            break

if __name__ == "__main__":
    run_all_tests()
