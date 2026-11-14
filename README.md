# 📊 E-Commerce Data Engineering and Analytics Dashboard

## 📌 Project Overview

The **E-Commerce Data Engineering and Analytics Dashboard** is an interactive data engineering project developed using Python, PySpark, SQL, and Streamlit.

The project processes e-commerce datasets, performs data cleaning, joins multiple datasets, calculates sales metrics, and generates meaningful business insights through an interactive dashboard.

It demonstrates practical data engineering concepts and data analysis techniques used in real-world applications.

## 🚀 Technologies Used

- **Python** – Programming and data processing
- **PySpark** – Large-scale data processing and transformations
- **SQL** – Data querying and analysis
- **Pandas** – Dataset creation and manipulation
- **Streamlit** – Interactive dashboard development
- **Plotly** – Data visualization
- **Parquet** – Processed data storage
- **Docker** – Application containerization

## 📂 Project Structure

```text
Ecommerce_Project/
│
├── app.py
├── requirements.txt
├── Dockerfile
│
├── data/
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
│
└── output/
```

## ⚙️ Key Features

### 1. Data Generation
- Creates sample customer, product, and order datasets.
- Stores datasets in CSV format.

### 2. Data Processing Using PySpark
- Reads multiple CSV datasets.
- Handles missing values and duplicate records.
- Performs joins and transformations.
- Calculates revenue using quantity and product price.
- Performs aggregations using `groupBy()`.

### 3. SQL Analysis
- Total revenue calculation.
- Top 5 customers by revenue.
- City-wise sales analysis.
- Second-highest product revenue.
- Customers without orders.
- Monthly revenue analysis.

### 4. PySpark Window Functions
- Product ranking using `DENSE_RANK()`.
- Revenue-based product comparison.

### 5. Interactive Streamlit Dashboard

The dashboard contains five sections:

- Dashboard – Overall sales metrics and visualizations.
- Customer Analysis – Customer revenue and order analysis.
- Product Analysis – Product sales and rankings.
- SQL Analysis – Execute predefined SQL queries.
- PySpark Processing – View processed datasets and download results.

### 6. Data Export
- Saves processed datasets in Parquet format.
- Downloads processed sales data as CSV.

## 🛠️ Installation and Setup

### Step 1: Clone the repository

```bash
git clone YOUR_GITHUB_REPOSITORY_URL
```

### Step 2: Navigate to the project folder

```bash
cd Ecommerce_Project
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

**Requirements:**
- Python 3.11
- Java 17
- PySpark
- Streamlit
- Pandas
- Plotly

### Step 4: Run the application

```bash
streamlit run app.py
```

### Step 5: Open the dashboard

```text
http://localhost:8501
```

## 🐳 Docker Deployment

Build the Docker image:

```bash
docker build -t ecommerce-dashboard .
```

Run the container:

```bash
docker run -p 8501:8501 ecommerce-dashboard
```

Open the application:

```text
http://localhost:8501
```

## 📈 Business Insights

This project helps analyze:

- Total e-commerce revenue.
- Customer purchasing behavior.
- Best-performing products.
- City-wise sales performance.
- Monthly revenue trends.
- Product revenue rankings.

## 🎯 Learning Outcomes

Through this project, I gained practical experience in:

- PySpark DataFrame operations.
- Data cleaning and preprocessing.
- SQL queries and window functions.
- Joining multiple datasets.
- Data aggregation and transformation.
- Interactive dashboard development.
- Data storage using Parquet.
- Docker-based application deployment.

## 🔮 Future Enhancements

- MySQL database integration.
- Real-world e-commerce datasets.
- Incremental data loading.
- Apache Airflow workflow orchestration.
- Advanced sales forecasting.
- Cloud-based data storage.

## 👨‍💻 Author

**Akshay**

Data Engineering | Python | SQL | PySpark | Machine Learning

---

⭐ If you find this project useful, feel free to explore and contribute!
