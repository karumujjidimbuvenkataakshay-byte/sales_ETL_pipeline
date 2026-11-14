
import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime

# ------------------------------------------------
# PAGE CONFIGURATION
# ------------------------------------------------

st.set_page_config(
    page_title="Retail Sales ETL Pipeline",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Retail Sales ETL Pipeline")
st.write(
    "An end-to-end data engineering project using "
    "Python, Pandas and SQL."
)

DB_PATH = "retail_sales.db"

# ------------------------------------------------
# SAMPLE DATA GENERATION
# ------------------------------------------------

def generate_sample_data():

    data = {
        "order_id": [101, 102, 103, 104, 105, 106, 107, 108,
                     109, 110, 111, 112],
        "order_date": [
            "2026-01-05", "2026-01-08", "2026-01-15",
            "2026-02-02", "2026-02-10", "2026-02-18",
            "2026-03-01", "2026-03-09", "2026-03-15",
            "2026-04-03", "2026-04-12", "2026-04-20"
        ],
        "customer_id": [
            1, 2, 3, 1, 4, 5, 2, 6, 3, 4, 5, 1
        ],
        "product": [
            "Laptop", "Mobile", "Headphones", "Laptop",
            "Keyboard", "Mouse", "Mobile", "Monitor",
            "Headphones", "Laptop", "Mouse", "Mobile"
        ],
        "category": [
            "Electronics", "Electronics", "Accessories",
            "Electronics", "Accessories", "Accessories",
            "Electronics", "Electronics", "Accessories",
            "Electronics", "Accessories", "Electronics"
        ],
        "quantity": [1, 2, 3, 1, 2, 4, 1, 2, 2, 1, 3, 2],
        "unit_price": [
            50000, 20000, 2000, 50000, 1500, 800,
            20000, 15000, 2000, 50000, 800, 20000
        ],
        "region": [
            "South", "North", "East", "South",
            "West", "North", "North", "South",
            "East", "West", "North", "South"
        ]
    }

    return pd.DataFrame(data)


# ------------------------------------------------
# EXTRACT
# ------------------------------------------------

def extract_data(uploaded_file):

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
    else:
        df = generate_sample_data()

    return df


# ------------------------------------------------
# TRANSFORM
# ------------------------------------------------

def transform_data(df):

    required_columns = [
        "order_id",
        "order_date",
        "customer_id",
        "product",
        "category",
        "quantity",
        "unit_price",
        "region"
    ]

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing columns: " + ", ".join(missing_columns)
        )

    df = df[required_columns].copy()

    original_count = len(df)

    # Remove duplicate orders
    df = df.drop_duplicates(subset=["order_id"])

    # Convert data types
    df["order_date"] = pd.to_datetime(
        df["order_date"], errors="coerce"
    )

    numeric_columns = [
        "order_id", "customer_id", "quantity", "unit_price"
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column], errors="coerce"
        )

    # Remove records with missing required values
    df = df.dropna(subset=required_columns)

    # Remove invalid records
    df = df[
        (df["quantity"] > 0) &
        (df["unit_price"] >= 0) &
        (df["order_id"] > 0)
    ]

    # Convert numeric columns to integer where appropriate
    df["order_id"] = df["order_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["quantity"] = df["quantity"].astype(int)

    # Calculate revenue
    df["total_amount"] = (
        df["quantity"] * df["unit_price"]
    )

    # Extract month
    df["month"] = df["order_date"].dt.strftime("%Y-%m")

    # Format date
    df["order_date"] = df["order_date"].dt.strftime("%Y-%m-%d")

    removed_count = original_count - len(df)

    return df, original_count, removed_count


# ------------------------------------------------
# LOAD INTO SQLITE DATABASE
# ------------------------------------------------

def load_data(df):

    connection = sqlite3.connect(DB_PATH)

    df.to_sql(
        "sales",
        connection,
        if_exists="replace",
        index=False
    )

    connection.close()


# ------------------------------------------------
# READ DATA FROM DATABASE
# ------------------------------------------------

def read_sql(query):

    connection = sqlite3.connect(DB_PATH)

    result = pd.read_sql_query(query, connection)

    connection.close()

    return result


# ------------------------------------------------
# SIDEBAR
# ------------------------------------------------

st.sidebar.title("Navigation")

page = st.sidebar.radio(
    "Choose Section",
    [
        "ETL Pipeline",
        "Dashboard",
        "SQL Analytics",
        "Data Quality",
        "Database"
    ]
)

st.sidebar.divider()

uploaded_file = st.sidebar.file_uploader(
    "Upload Sales CSV",
    type=["csv"]
)

st.sidebar.caption(
    "Required CSV columns: order_id, order_date, "
    "customer_id, product, category, quantity, "
    "unit_price, region"
)


# ------------------------------------------------
# ETL PIPELINE
# ------------------------------------------------

if page == "ETL Pipeline":

    st.header("⚙️ Execute ETL Pipeline")

    st.write("Follow the three stages of data engineering.")

    col1, col2, col3 = st.columns(3)

    col1.info("1. EXTRACT\n\nRead CSV data")
    col2.info("2. TRANSFORM\n\nClean and process data")
    col3.info("3. LOAD\n\nStore data in SQLite")

    if st.button("Run Complete ETL Pipeline", type="primary"):

        try:

            # Extract
            raw_df = extract_data(uploaded_file)

            # Transform
            cleaned_df, original, removed = transform_data(
                raw_df
            )

            if cleaned_df.empty:
                st.error("No valid records found.")
            else:

                # Load
                load_data(cleaned_df)

                st.session_state["raw_data"] = raw_df
                st.session_state["clean_data"] = cleaned_df
                st.session_state["original_count"] = original
                st.session_state["removed_count"] = removed

                st.success("ETL Pipeline completed successfully!")

                a, b, c = st.columns(3)

                a.metric("Extracted Records", original)
                b.metric("Clean Records", len(cleaned_df))
                c.metric("Removed Records", removed)

                st.subheader("Processed Data")

                st.dataframe(
                    cleaned_df,
                    use_container_width=True
                )

        except Exception as error:
            st.error(f"ETL Error: {error}")


# ------------------------------------------------
# DASHBOARD
# ------------------------------------------------

elif page == "Dashboard":

    st.header("📈 Business Dashboard")

    if os.path.exists(DB_PATH):

        sales_df = read_sql("SELECT * FROM sales")

        total_revenue = sales_df["total_amount"].sum()
        total_orders = sales_df["order_id"].nunique()
        total_customers = sales_df["customer_id"].nunique()
        total_quantity = sales_df["quantity"].sum()

        c1, c2, c3, c4 = st.columns(4)

        c1.metric("Total Revenue", f"₹{total_revenue:,.0f}")
        c2.metric("Total Orders", total_orders)
        c3.metric("Customers", total_customers)
        c4.metric("Units Sold", total_quantity)

        st.subheader("Revenue by Region")

        region_sales = (
            sales_df.groupby("region")["total_amount"]
            .sum()
            .sort_values(ascending=False)
        )

        st.bar_chart(region_sales)

        st.subheader("Monthly Revenue")

        monthly_sales = (
            sales_df.groupby("month")["total_amount"]
            .sum()
            .sort_index()
        )

        st.line_chart(monthly_sales)

        st.subheader("Product Revenue")

        product_sales = (
            sales_df.groupby("product")["total_amount"]
            .sum()
            .sort_values(ascending=False)
        )

        st.bar_chart(product_sales)

    else:
        st.warning("Please run the ETL pipeline first.")


# ------------------------------------------------
# SQL ANALYTICS
# ------------------------------------------------

elif page == "SQL Analytics":

    st.header("🗄️ SQL Business Analytics")

    if os.path.exists(DB_PATH):

        queries = {

            "Total Revenue": """
                SELECT SUM(total_amount) AS total_revenue
                FROM sales
            """,

            "Top Selling Products": """
                SELECT product,
                       SUM(quantity) AS total_quantity
                FROM sales
                GROUP BY product
                ORDER BY total_quantity DESC
            """,

            "Revenue by Region": """
                SELECT region,
                       SUM(total_amount) AS revenue
                FROM sales
                GROUP BY region
                ORDER BY revenue DESC
            """,

            "Top Customers": """
                SELECT customer_id,
                       SUM(total_amount) AS total_spent
                FROM sales
                GROUP BY customer_id
                ORDER BY total_spent DESC
            """,

            "Monthly Revenue": """
                SELECT month,
                       SUM(total_amount) AS revenue
                FROM sales
                GROUP BY month
                ORDER BY month
            """,

            "Category Analysis": """
                SELECT category,
                       SUM(total_amount) AS revenue,
                       SUM(quantity) AS quantity_sold
                FROM sales
                GROUP BY category
                ORDER BY revenue DESC
            """
        }

        selected_query = st.selectbox(
            "Select SQL Query",
            list(queries.keys())
        )

        st.code(queries[selected_query], language="sql")

        if st.button("Execute SQL Query"):

            result = read_sql(queries[selected_query])

            st.subheader("Query Result")

            st.dataframe(
                result,
                use_container_width=True
            )

    else:
        st.warning("Run the ETL pipeline first.")


# ------------------------------------------------
# DATA QUALITY
# ------------------------------------------------

elif page == "Data Quality":

    st.header("🔍 Data Quality Report")

    if os.path.exists(DB_PATH):

        df = read_sql("SELECT * FROM sales")

        st.subheader("Data Quality Summary")

        col1, col2, col3 = st.columns(3)

        col1.metric("Valid Records", len(df))
        col2.metric("Missing Values", int(df.isna().sum().sum()))
        col3.metric("Duplicate Order IDs", int(df["order_id"].duplicated().sum()))

        st.subheader("Column Information")

        information = pd.DataFrame({
            "Column": df.columns,
            "Data Type": df.dtypes.astype(str).values,
            "Missing Values": df.isna().sum().values
        })

        st.dataframe(
            information,
            use_container_width=True
        )

        st.subheader("Cleaned Dataset")

        st.dataframe(df, use_container_width=True)

    else:
        st.warning("Run the ETL pipeline first.")


# ------------------------------------------------
# DATABASE
# ------------------------------------------------

elif page == "Database":

    st.header("💾 SQLite Database")

    if os.path.exists(DB_PATH):

        df = read_sql("SELECT * FROM sales")

        st.success("SQLite database is available.")

        st.write("Database table: sales")

        st.dataframe(df, use_container_width=True)

        st.download_button(
            "Download Processed CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="cleaned_retail_sales.csv",
            mime="text/csv"
        )

    else:
        st.warning("Database not created. Run the ETL pipeline first.")


# ------------------------------------------------
# FOOTER
# ------------------------------------------------

st.divider()

st.caption(
    "Retail Sales ETL Pipeline | Python | Pandas | SQL | SQLite | Streamlit"
)
