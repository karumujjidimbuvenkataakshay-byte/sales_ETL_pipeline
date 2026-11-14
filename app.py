
import os
import streamlit as st
import pandas as pd
import plotly.express as px

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, desc, dense_rank, round
)
from pyspark.sql.window import Window

# =====================================
# PAGE CONFIGURATION
# =====================================

st.set_page_config(
    page_title="E-Commerce Data Engineering",
    page_icon="📊",
    layout="wide"
)

st.title("📊 E-Commerce Data Engineering Dashboard")
st.caption("Data processing and analytics using PySpark and SQL")

# =====================================
# CREATE SAMPLE DATASETS
# =====================================

os.makedirs("data", exist_ok=True)
os.makedirs("output", exist_ok=True)

customers = pd.DataFrame({
    "customer_id": [1, 2, 3, 4, 5, 6],
    "name": ["Akash", "Rahul", "Priya", "Sneha", "Arjun", "Kiran"],
    "city": ["Hyderabad", "Chennai", "Bangalore",
             "Mumbai", "Delhi", "Hyderabad"]
})

products = pd.DataFrame({
    "product_id": [101, 102, 103, 104, 105],
    "product_name": ["Laptop", "Mobile", "Headphones",
                     "Keyboard", "Mouse"],
    "category": ["Electronics", "Electronics",
                 "Accessories", "Accessories", "Accessories"],
    "price": [50000, 20000, 2000, 1500, 800]
})

orders = pd.DataFrame({
    "order_id": [1001, 1002, 1003, 1004, 1005,
                 1006, 1007, 1008],
    "customer_id": [1, 2, 3, 1, 4, 5, 2, 3],
    "product_id": [101, 102, 103, 104, 105, 102, 101, 103],
    "quantity": [1, 2, 3, 2, 1, 1, 1, 2],
    "order_date": [
        "2026-01-05", "2026-01-10", "2026-01-15",
        "2026-02-02", "2026-02-10", "2026-02-15",
        "2026-03-01", "2026-03-05"
    ]
})

customers.to_csv("data/customers.csv", index=False)
products.to_csv("data/products.csv", index=False)
orders.to_csv("data/orders.csv", index=False)

# =====================================
# CREATE SPARK SESSION
# =====================================

@st.cache_resource
def create_spark():
    return (
        SparkSession.builder
        .appName("Ecommerce_Streamlit")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

spark = create_spark()
spark.sparkContext.setLogLevel("ERROR")

# =====================================
# READ DATA USING PYSPARK
# =====================================

customers_df = spark.read.csv(
    "data/customers.csv",
    header=True,
    inferSchema=True
)

products_df = spark.read.csv(
    "data/products.csv",
    header=True,
    inferSchema=True
)

orders_df = spark.read.csv(
    "data/orders.csv",
    header=True,
    inferSchema=True
)

# =====================================
# DATA CLEANING
# =====================================

customers_df = customers_df.dropDuplicates().dropna()
products_df = products_df.dropDuplicates().dropna()
orders_df = orders_df.dropDuplicates().dropna()

orders_df = orders_df.withColumn(
    "order_date", col("order_date").cast("date")
)

# =====================================
# JOIN DATASETS
# =====================================

sales_df = orders_df.join(
    customers_df, "customer_id", "inner"
).join(
    products_df, "product_id", "inner"
)

sales_df = sales_df.withColumn(
    "revenue", col("quantity") * col("price")
)

sales_df.createOrReplaceTempView("sales")
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")

# =====================================
# SIDEBAR
# =====================================

st.sidebar.title("Navigation")

page = st.sidebar.radio(
    "Select Page",
    [
        "Dashboard",
        "Customer Analysis",
        "Product Analysis",
        "SQL Analysis",
        "PySpark Processing"
    ]
)

# =====================================
# DASHBOARD
# =====================================

if page == "Dashboard":

    st.header("Sales Overview")

    total_revenue = sales_df.agg(
        sum("revenue")
    ).first()[0]

    total_orders = sales_df.select(
        "order_id"
    ).distinct().count()

    total_customers = customers_df.count()
    total_products = products_df.count()

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Total Revenue", f"₹{total_revenue:,.0f}")
    c2.metric("Total Orders", total_orders)
    c3.metric("Customers", total_customers)
    c4.metric("Products", total_products)

    st.divider()

    sales_pd = sales_df.toPandas()

    st.subheader("Revenue by City")

    city_sales = sales_pd.groupby(
        "city", as_index=False
    )["revenue"].sum()

    fig = px.bar(
        city_sales,
        x="city",
        y="revenue",
        title="City-wise Revenue",
        color="city"
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Monthly Sales")

    sales_pd["order_date"] = pd.to_datetime(
        sales_pd["order_date"]
    )

    sales_pd["month"] = sales_pd[
        "order_date"
    ].dt.strftime("%Y-%m")

    monthly = sales_pd.groupby(
        "month", as_index=False
    )["revenue"].sum()

    fig2 = px.line(
        monthly,
        x="month",
        y="revenue",
        markers=True,
        title="Monthly Revenue Trend"
    )

    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Sales Data")
    st.dataframe(sales_pd, use_container_width=True)

# =====================================
# CUSTOMER ANALYSIS
# =====================================

elif page == "Customer Analysis":

    st.header("Customer Analysis")

    customer_sales = spark.sql("""
        SELECT name, city,
               SUM(revenue) AS total_revenue,
               COUNT(DISTINCT order_id) AS total_orders
        FROM sales
        GROUP BY name, city
        ORDER BY total_revenue DESC
    """)

    st.dataframe(
        customer_sales.toPandas(),
        use_container_width=True
    )

    st.subheader("Top 5 Customers")

    st.dataframe(
        customer_sales.limit(5).toPandas(),
        use_container_width=True
    )

    st.subheader("Customer Dataset")
    st.dataframe(customers_df.toPandas())

# =====================================
# PRODUCT ANALYSIS
# =====================================

elif page == "Product Analysis":

    st.header("Product Analysis")

    product_sales = spark.sql("""
        SELECT product_name, category,
               SUM(quantity) AS total_quantity,
               SUM(revenue) AS total_revenue
        FROM sales
        GROUP BY product_name, category
        ORDER BY total_revenue DESC
    """)

    product_pd = product_sales.toPandas()

    st.dataframe(product_pd, use_container_width=True)

    fig = px.bar(
        product_pd,
        x="product_name",
        y="total_revenue",
        color="category",
        title="Product-wise Revenue"
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Product Ranking")

    window = Window.orderBy(desc("total_revenue"))

    ranked = product_sales.withColumn(
        "rank",
        dense_rank().over(window)
    )

    st.dataframe(ranked.toPandas(), use_container_width=True)

# =====================================
# SQL ANALYSIS
# =====================================

elif page == "SQL Analysis":

    st.header("SQL Analysis")

    queries = {
        "Total Revenue": """
            SELECT SUM(revenue) AS total_revenue
            FROM sales
        """,

        "Top Customers": """
            SELECT name, SUM(revenue) AS total_revenue
            FROM sales
            GROUP BY name
            ORDER BY total_revenue DESC
            LIMIT 5
        """,

        "City-wise Sales": """
            SELECT city, SUM(revenue) AS total_sales
            FROM sales
            GROUP BY city
            ORDER BY total_sales DESC
        """,

        "Second Highest Product Revenue": """
            WITH ranked AS (
                SELECT product_name,
                       SUM(revenue) AS total_revenue,
                       DENSE_RANK() OVER (
                           ORDER BY SUM(revenue) DESC
                       ) AS rnk
                FROM sales
                GROUP BY product_name
            )
            SELECT * FROM ranked WHERE rnk = 2
        """,

        "Customers Without Orders": """
            SELECT c.customer_id, c.name
            FROM customers c
            LEFT JOIN orders o
            ON c.customer_id = o.customer_id
            WHERE o.customer_id IS NULL
        """,

        "Monthly Revenue": """
            SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS month,
                   SUM(revenue) AS monthly_sales
            FROM sales
            GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
            ORDER BY month
        """
    }

    selected_query = st.selectbox(
        "Choose SQL Query",
        list(queries.keys())
    )

    st.code(queries[selected_query], language="sql")

    if st.button("Execute SQL Query"):
        result = spark.sql(queries[selected_query])
        st.success("Query executed successfully!")
        st.dataframe(result.toPandas(), use_container_width=True)

# =====================================
# PYSPARK PROCESSING
# =====================================

elif page == "PySpark Processing":

    st.header("PySpark Data Processing")

    st.subheader("Customer Data")
    st.dataframe(customers_df.toPandas())

    st.subheader("Product Data")
    st.dataframe(products_df.toPandas())

    st.subheader("Order Data")
    st.dataframe(orders_df.toPandas())

    st.subheader("Joined and Transformed Data")
    st.dataframe(sales_df.toPandas(), use_container_width=True)

    if st.button("Save Processed Data"):

        sales_df.write.mode("overwrite").parquet(
            "output/ecommerce_sales"
        )

        st.success("Parquet data saved successfully!")

    csv_data = sales_df.toPandas().to_csv(index=False)

    st.download_button(
        "Download Processed CSV",
        data=csv_data,
        file_name="ecommerce_sales.csv",
        mime="text/csv"
    )

# =====================================
# FOOTER
# =====================================

st.sidebar.divider()
st.sidebar.caption("Built with Python, PySpark, SQL & Streamlit")
