
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# ==========================================
# 1. DASHBOARD CONFIGURATION
# ==========================================

st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="🛒",
    layout="wide"
)

# ==========================================
# 2. PROJECT PATH
# ==========================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "reports"

# ==========================================
# 3. LOAD GOLD DATASETS
# ==========================================

def load_data(filename):
    file_path = DATA_PATH / filename

    if not file_path.exists():
        return pd.DataFrame()

    return pd.read_csv(file_path)


monthly = load_data("monthly_sales.csv")
products = load_data("product_sales.csv")
cities = load_data("city_sales.csv")
customers = load_data("customer_sales.csv")
payments = load_data("payment_analysis.csv")
delivery = load_data("delivery_analysis.csv")

datasets = {
    "Monthly Sales": monthly,
    "Product Sales": products,
    "City Sales": cities,
    "Customer Sales": customers,
    "Payment Analysis": payments,
    "Delivery Analysis": delivery
}

# ==========================================
# 4. HELPER FUNCTIONS
# ==========================================

def find_column(df, keywords):
    """Find a column using possible column-name keywords."""

    for column in df.columns:
        name = column.lower().replace(" ", "_")

        if any(word in name for word in keywords):
            return column

    return None


def show_dataset_status():
    missing = [
        name for name, df in datasets.items()
        if df.empty
    ]

    if missing:
        st.error(
            "Missing or empty datasets: " + ", ".join(missing)
        )
        st.info(
            "Check the CSV filenames and the reports folder."
        )
        st.stop()


# ==========================================
# 5. DASHBOARD HEADER
# ==========================================

st.title("🛒 E-Commerce Analytics Dashboard")

st.markdown(
    "Business insights powered by **PySpark, Databricks, "
    "Delta Lake and SQL**."
)

st.markdown("---")

show_dataset_status()

# ==========================================
# 6. OVERVIEW METRICS
# ==========================================

st.header("📊 Business Overview")

order_col = find_column(monthly, ["total_orders", "order_count", "orders"])
revenue_col = find_column(monthly, ["revenue", "sales", "total_sales"])
customer_col = find_column(customers, ["customer_id"])
product_col = find_column(products, ["product_id"])

total_orders = (
    pd.to_numeric(monthly[order_col], errors="coerce").sum()
    if order_col else 0
)

total_revenue = (
    pd.to_numeric(monthly[revenue_col], errors="coerce").sum()
    if revenue_col else 0
)

total_customers = (
    customers[customer_col].nunique()
    if customer_col else len(customers)
)

total_products = (
    products[product_col].nunique()
    if product_col else len(products)
)

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Orders", f"{total_orders:,.0f}")
col2.metric("Total Revenue", f"₹ {total_revenue:,.2f}")
col3.metric("Total Customers", f"{total_customers:,.0f}")
col4.metric("Products", f"{total_products:,.0f}")

st.markdown("---")

# ==========================================
# 7. MONTHLY SALES ANALYSIS
# ==========================================

st.header("📅 Monthly Sales Analysis")

if not monthly.empty:

    date_col = find_column(
        monthly,
        ["month", "date", "year_month"]
    )

    if date_col and revenue_col:

        chart_df = monthly.copy()
        chart_df[revenue_col] = pd.to_numeric(
            chart_df[revenue_col], errors="coerce"
        )

        fig = px.bar(
            chart_df,
            x=date_col,
            y=revenue_col,
            title="Monthly Revenue",
            text_auto=".2s",
            template="plotly_white"
        )

        fig.update_layout(
            xaxis_title="Month",
            yaxis_title="Revenue",
            hovermode="x unified"
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info(
            "Monthly chart requires a month/date column "
            "and a revenue/sales column."
        )

st.markdown("---")

# ==========================================
# 8. PRODUCT SALES ANALYSIS
# ==========================================

st.header("🏆 Product Performance")

if not products.empty:

    product_name = find_column(
        products,
        ["product_name", "product_category", "product_id", "category"]
    )

    product_revenue = find_column(
        products,
        ["revenue", "sales", "total_sales", "total_revenue"]
    )

    if product_name and product_revenue:

        chart_df = products.copy()
        chart_df[product_revenue] = pd.to_numeric(
            chart_df[product_revenue], errors="coerce"
        )

        chart_df = chart_df.nlargest(10, product_revenue)

        fig = px.bar(
            chart_df,
            x=product_name,
            y=product_revenue,
            title="Top 10 Products by Revenue",
            template="plotly_white"
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Product chart requires product and revenue columns.")

st.markdown("---")

# ==========================================
# 9. CITY SALES ANALYSIS
# ==========================================

st.header("🌍 City Performance")

if not cities.empty:

    city_col = find_column(
        cities,
        ["city", "customer_city"]
    )

    city_revenue = find_column(
        cities,
        ["revenue", "sales", "total_sales"]
    )

    if city_col and city_revenue:

        chart_df = cities.copy()
        chart_df[city_revenue] = pd.to_numeric(
            chart_df[city_revenue], errors="coerce"
        )

        chart_df = chart_df.nlargest(10, city_revenue)

        fig = px.bar(
            chart_df,
            x=city_col,
            y=city_revenue,
            title="Top 10 Cities by Revenue",
            template="plotly_white"
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("City chart requires city and revenue columns.")

st.markdown("---")

# ==========================================
# 10. CUSTOMER ANALYSIS
# ==========================================

st.header("👥 Customer Analysis")

if not customers.empty:

    spending_col = find_column(
        customers,
        ["total_spent", "total_revenue", "customer_spending", "revenue"]
    )

    if spending_col:

        chart_df = customers.copy()
        chart_df[spending_col] = pd.to_numeric(
            chart_df[spending_col], errors="coerce"
        )

        top_customers = chart_df.nlargest(10, spending_col)

        fig = px.bar(
            top_customers,
            x=spending_col,
            title="Top 10 Customers by Spending",
            template="plotly_white"
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Customer spending column was not detected.")

st.markdown("---")

# ==========================================
# 11. PAYMENT ANALYSIS
# ==========================================

st.header("💳 Payment Analysis")

if not payments.empty:

    payment_type = find_column(
        payments,
        ["payment_type", "payment_method"]
    )

    payment_value = find_column(
        payments,
        ["payment_value", "total_payment", "amount", "revenue"]
    )

    if payment_type and payment_value:

        chart_df = payments.copy()
        chart_df[payment_value] = pd.to_numeric(
            chart_df[payment_value], errors="coerce"
        )

        fig = px.pie(
            chart_df,
            names=payment_type,
            values=payment_value,
            title="Payment Method Distribution",
            hole=0.4
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Payment type or payment value column was not detected.")

st.markdown("---")

# ==========================================
# 12. DELIVERY ANALYSIS
# ==========================================

st.header("🚚 Delivery Analysis")

if not delivery.empty:

    delivery_col = find_column(
        delivery,
        ["delivery_days", "average_delivery", "delivery_time"]
    )

    status_col = find_column(
        delivery,
        ["delivery_status", "order_status"]
    )

    if status_col:

        status_counts = delivery[status_col].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]

        fig = px.bar(
            status_counts,
            x="Status",
            y="Count",
            title="Delivery Status Distribution",
            template="plotly_white"
        )

        st.plotly_chart(fig, use_container_width=True)

    elif delivery_col:

        chart_df = delivery.copy()
        chart_df[delivery_col] = pd.to_numeric(
            chart_df[delivery_col], errors="coerce"
        )

        st.metric(
            "Average Delivery Time",
            f"{chart_df[delivery_col].mean():.2f} days"
        )

    else:
        st.info("No recognized delivery metric or status column found.")

st.markdown("---")

# ==========================================
# 13. DATASET EXPLORER
# ==========================================

st.header("📁 Gold Dataset Explorer")

selected_dataset = st.selectbox(
    "Select a dataset",
    list(datasets.keys())
)

selected_df = datasets[selected_dataset]

st.write(
    f"**Rows:** {len(selected_df):,} | "
    f"**Columns:** {len(selected_df.columns)}"
)

st.dataframe(selected_df)

# ==========================================
# 14. FOOTER
# ==========================================

st.markdown("---")

st.markdown(
    """
    <div style="text-align:center; color:gray;">
        E-Commerce Data Engineering Project<br>
        Built with Python | PySpark | Databricks | Delta Lake |
        SQL | Streamlit | Plotly
    </div>
    """,
    unsafe_allow_html=True
)
