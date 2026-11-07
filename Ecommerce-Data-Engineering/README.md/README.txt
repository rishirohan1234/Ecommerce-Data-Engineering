# E-Commerce Data Engineering Project

## Project Overview

An end-to-end E-Commerce Data Engineering and Analytics
project built using AWS, Databricks, PySpark, SQL,
Delta Lake and Streamlit.

## Technologies Used

- Python
- PySpark
- Databricks
- AWS
- Delta Lake
- SQL
- Streamlit
- Plotly

## Architecture

1. Raw data ingestion
2. Data cleaning using PySpark
3. Silver layer transformations
4. Gold layer aggregations
5. Incremental loading using Delta Lake MERGE
6. Business analysis using SQL
7. Interactive dashboard using Streamlit

## Project Structure

- data/ - Raw data location
- python/ - Python scripts
- notebooks/ - Data engineering pipeline
- sql/ - SQL analytics queries
- reports/ - Gold datasets
- dashboard/ - Streamlit dashboard

## Dashboard

Displays:

- Monthly sales
- Product performance
- City performance
- Customer analysis
- Payment analysis
- Delivery analysis

## How to Run

1. Clone the repository.
2. Create a virtual environment.
3. Install requirements.
4. Run the dashboard.

```bash
python -m pip install -r requirements.txt
python -m streamlit run dashboard/app.py
```
