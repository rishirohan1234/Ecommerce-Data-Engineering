import pandas as pd
from pathlib import Path

# Location of our raw dataset
DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "raw"

# List of CSV files
files = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_sellers_dataset.csv"
]

# Inspect every dataset
for file in files:

    print("\n------------------------------")
    print("Dataset:", file)

    file_path = DATA_PATH / file

    df = pd.read_csv(file_path)

    # Number of rows and columns
    print("Shape:", df.shape)

    # Column names
    print("Columns:", list(df.columns))

    # Missing values
    print("Missing values:")
    print(df.isnull().sum())

    # Duplicate rows
    print("Duplicate rows:", df.duplicated().sum())

    # Display first 5 records
    print("First 5 records:")
    print(df.head())