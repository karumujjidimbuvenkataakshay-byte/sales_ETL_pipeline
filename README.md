
# Retail Sales ETL Pipeline

## Project Overview

The Retail Sales ETL Pipeline is an end-to-end Data Engineering project developed using Python, Pandas, SQL, SQLite and Streamlit.

The project extracts raw sales data, cleans and transforms the data, stores the processed records in a SQLite database, and performs business analytics using SQL queries.

## Technologies Used

- Python
- Pandas
- SQL
- SQLite
- Streamlit
- Data Cleaning
- ETL Pipeline

## Project Architecture

Extract → Transform → Load → SQL Analytics → Dashboard

## Project Features

### 1. Data Extraction
- Read sales data from CSV files.
- Generate sample sales data.
- Support CSV file uploads.

### 2. Data Transformation
- Remove duplicate orders.
- Handle missing values.
- Convert data types.
- Validate sales records.
- Calculate total sales revenue.
- Extract monthly information.

### 3. Data Loading
- Store processed data in SQLite.
- Create a sales database table.
- Retrieve processed data using SQL.

### 4. SQL Analytics
- Total revenue.
- Top-selling products.
- Revenue by region.
- Top customers.
- Monthly revenue.
- Category analysis.

### 5. Dashboard
- Total revenue.
- Total orders.
- Total customers.
- Units sold.
- Revenue charts.
- Product performance.

### 6. Data Quality
- Missing value checking.
- Duplicate checking.
- Data type information.
- Valid record count.

## Project Structure

retail-sales-etl/
│
├── app.py
├── requirements.txt
├── README.md
└── retail_sales.db (generated automatically)

## Installation

Install the dependencies:

pip install -r requirements.txt

## Run the Application

streamlit run app.py

## ETL Workflow

1. Extract sales records from CSV.
2. Transform and clean the data using Pandas.
3. Calculate total sales.
4. Load processed data into SQLite.
5. Execute SQL queries.
6. Display business insights through Streamlit.

## Learning Outcomes

- Understanding ETL architecture.
- Building data pipelines using Python.
- Data cleaning and transformation using Pandas.
- Working with SQL queries.
- Loading data into SQLite databases.
- Creating data analytics dashboards.
- Understanding data quality checks.

## Future Enhancements

- Integrate Apache Airflow.
- Use PySpark for large-scale processing.
- Connect PostgreSQL.
- Add automated scheduled ETL jobs.
- Integrate cloud storage.

## Author

Developed as a Data Engineering portfolio project.
