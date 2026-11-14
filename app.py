
import streamlit as st
import pandas as pd
import os

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count,
    dense_rank, desc
)
from pyspark.sql.window import Window

# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="E-Commerce Data Analytics",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 E-Commerce Data Engineering Project")
st.write("Analyze customers, products, orders and sales using PySpark and SQL.")

# --------------------------------------------------
# SAMPLE DATA
# --------------------------------------------------

os.makedirs("data", exist_ok=True)

customers_data = {
    "customer_id": [1, 2, 3, 4, 5, 6],
    "customer_name": [
        "Akash", "Rahul", "Priya", "Sneha", "Arun", "Kiran"
    ],
    "city": [
        "Chennai", "Bangalore", "Hyderabad",
        "Chennai", "Mumbai", "Bangalore"
    ]
}

products_data = {
    "product_id": [101, 102, 103, 104, 105],
    "product_name": [
        "Laptop", "Mobile", "Headphones", "Keyboard", "Mouse"
    ],
    "price": [50000, 20000, 2000, 1500, 800]
}

orders_data = {
    "order_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
    "customer_id": [1, 2, 3, 1, 4, 5, 2, 6],
    "product_id": [101, 102, 103, 104, 105, 101, 103, 102],
    "quantity": [1, 2, 3, 2, 4, 1, 2, 1],
    "order_date": [
        "2026-01-10", "2026-01-15",
        "2026-02-05", "2026-02-12",
        "2026-03-01", "2026-03-10",
        "2026-04-05", "2026-04-15"
    ]
}

customers_pd = pd.DataFrame(customers_data)
products_pd = pd.DataFrame(products_data)
orders_pd = pd.DataFrame(orders_data)

# Save sample CSV files
customers_pd.to_csv("data/customers.csv", index=False)
products_pd.to_csv("data/products.csv", index=False)
orders_pd.to_csv("data/orders.csv", index=False)

# --------------------------------------------------
# PANDAS DATA PROCESSING
# --------------------------------------------------

sales_df = (
    orders_pd
    .merge(customers_pd, on="customer_id", how="left")
    .merge(products_pd, on="product_id", how="left")
)

sales_df["revenue"] = (
    sales_df["price"] * sales_df["quantity"]
)

sales_df["order_date"] = pd.to_datetime(
    sales_df["order_date"]
)

sales_df["month"] = sales_df["order_date"].dt.strftime("%Y-%m")

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

@st.cache_resource
def create_spark_session():
    return (
        SparkSession.builder
        .appName("EcommerceAnalytics")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

# --------------------------------------------------
# SIDEBAR
# --------------------------------------------------

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

# --------------------------------------------------
# DASHBOARD
# --------------------------------------------------

if page == "Dashboard":

    st.header("📊 Sales Dashboard")

    total_revenue = sales_df["revenue"].sum()
    total_orders = len(sales_df)
    total_customers = customers_pd["customer_id"].nunique()
    total_products = products_pd["product_id"].nunique()

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Total Revenue", f"₹{total_revenue:,.0f}")
    c2.metric("Total Orders", total_orders)
    c3.metric("Customers", total_customers)
    c4.metric("Products", total_products)

    st.subheader("Sales by City")

    city_sales = (
        sales_df.groupby("city")["revenue"]
        .sum()
        .sort_values(ascending=False)
    )

    st.bar_chart(city_sales)

    st.subheader("Monthly Revenue")

    monthly_sales = (
        sales_df.groupby("month")["revenue"]
        .sum()
        .sort_index()
    )

    st.line_chart(monthly_sales)

    st.subheader("Sales Data")

    st.dataframe(sales_df, use_container_width=True)

# --------------------------------------------------
# CUSTOMER ANALYSIS
# --------------------------------------------------

elif page == "Customer Analysis":

    st.header("👥 Customer Analysis")

    customer_sales = (
        sales_df.groupby(["customer_id", "customer_name", "city"])
        .agg(
            total_orders=("order_id", "count"),
            total_revenue=("revenue", "sum")
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    st.subheader("Customer Revenue")

    st.dataframe(customer_sales, use_container_width=True)

    st.subheader("Top Customers")

    st.bar_chart(
        customer_sales.set_index("customer_name")["total_revenue"]
    )

    st.subheader("Customer City Distribution")

    city_count = customers_pd["city"].value_counts()

    st.bar_chart(city_count)

# --------------------------------------------------
# PRODUCT ANALYSIS
# --------------------------------------------------

elif page == "Product Analysis":

    st.header("📦 Product Analysis")

    product_sales = (
        sales_df.groupby("product_name")
        .agg(
            total_quantity=("quantity", "sum"),
            total_revenue=("revenue", "sum")
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    st.subheader("Product Performance")

    st.dataframe(product_sales, use_container_width=True)

    st.subheader("Revenue by Product")

    st.bar_chart(
        product_sales.set_index("product_name")["total_revenue"]
    )

    st.subheader("Quantity Sold")

    st.bar_chart(
        product_sales.set_index("product_name")["total_quantity"]
    )

# --------------------------------------------------
# SQL ANALYSIS
# --------------------------------------------------

elif page == "SQL Analysis":

    st.header("🗄️ SQL Business Analysis")

    query_option = st.selectbox(
        "Choose SQL Analysis",
        [
            "Total Revenue",
            "Top Customers",
            "Product Revenue",
            "City-wise Sales",
            "Monthly Sales"
        ]
    )

    if query_option == "Total Revenue":

        result = pd.DataFrame({
            "Total Revenue": [sales_df["revenue"].sum()]
        })

    elif query_option == "Top Customers":

        result = (
            sales_df.groupby("customer_name")["revenue"]
            .sum()
            .reset_index()
            .sort_values("revenue", ascending=False)
        )

    elif query_option == "Product Revenue":

        result = (
            sales_df.groupby("product_name")["revenue"]
            .sum()
            .reset_index()
            .sort_values("revenue", ascending=False)
        )

    elif query_option == "City-wise Sales":

        result = (
            sales_df.groupby("city")["revenue"]
            .sum()
            .reset_index()
            .sort_values("revenue", ascending=False)
        )

    else:

        result = (
            sales_df.groupby("month")["revenue"]
            .sum()
            .reset_index()
            .sort_values("month")
        )

    st.dataframe(result, use_container_width=True)

# --------------------------------------------------
# PYSPARK PROCESSING
# --------------------------------------------------

elif page == "PySpark Processing":

    st.header("⚡ PySpark Data Processing")

    st.write(
        "This section demonstrates Spark DataFrames, joins, "
        "aggregations, window functions and Spark SQL."
    )

    if st.button("Run PySpark Processing"):

        try:
            spark = create_spark_session()

            customers_spark = spark.createDataFrame(customers_pd)
            products_spark = spark.createDataFrame(products_pd)
            orders_spark = spark.createDataFrame(orders_pd)

            # Join DataFrames
            spark_sales = (
                orders_spark
                .join(customers_spark, "customer_id")
                .join(products_spark, "product_id")
            )

            # Calculate revenue
            spark_sales = spark_sales.withColumn(
                "revenue",
                col("price") * col("quantity")
            )

            st.subheader("Joined Spark DataFrame")

            st.dataframe(
                spark_sales.toPandas(),
                use_container_width=True
            )

            # Aggregation
            st.subheader("Revenue by Product")

            product_revenue = (
                spark_sales
                .groupBy("product_name")
                .agg(
                    spark_sum("revenue").alias("total_revenue")
                )
                .orderBy(desc("total_revenue"))
            )

            st.dataframe(
                product_revenue.toPandas(),
                use_container_width=True
            )

            # Window function
            st.subheader("Product Ranking")

            window_spec = Window.orderBy(
                desc("total_revenue")
            )

            ranked_products = (
                product_revenue
                .withColumn(
                    "rank",
                    dense_rank().over(window_spec)
                )
            )

            st.dataframe(
                ranked_products.toPandas(),
                use_container_width=True
            )

            # Spark SQL
            st.subheader("Spark SQL Analysis")

            spark_sales.createOrReplaceTempView("sales")

            sql_result = spark.sql("""
                SELECT
                    city,
                    SUM(revenue) AS total_revenue
                FROM sales
                GROUP BY city
                ORDER BY total_revenue DESC
            """)

            st.write("SQL Query: Total Revenue by City")

            st.code("""
SELECT city, SUM(revenue) AS total_revenue
FROM sales
GROUP BY city
ORDER BY total_revenue DESC
            """)

            st.dataframe(
                sql_result.toPandas(),
                use_container_width=True
            )

            # Save Parquet
            spark_sales.write.mode("overwrite").parquet(
                "data/ecommerce_sales"
            )

            st.success("PySpark processing completed successfully!")

        except Exception as e:
            st.error("PySpark could not start or complete processing.")
            st.exception(e)

# --------------------------------------------------
# DOWNLOAD DATA
# --------------------------------------------------

st.sidebar.divider()

st.sidebar.download_button(
    "Download Sales CSV",
    data=sales_df.to_csv(index=False).encode("utf-8"),
    file_name="ecommerce_sales.csv",
    mime="text/csv"
)

st.sidebar.success("E-Commerce Analytics Project")
