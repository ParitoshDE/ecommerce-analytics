"""
Olist Brazilian E-Commerce — Analytics Dashboard
Two tiles:
  1. Monthly Orders & Revenue (bar + line combo)
  2. Revenue by Product Category (horizontal bar)

Reads from RDS PostgreSQL olist_prod schema.
Secrets loaded from Streamlit Cloud secrets or environment variables.
"""

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import psycopg2

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Olist E-Commerce Analytics",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 Olist Brazilian E-Commerce Analytics")
st.caption("Data: Sep 2016 – Oct 2018 · ~100k orders · Source: Kaggle olistbr/brazilian-ecommerce")

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
def get_conn():
    # Streamlit Cloud secrets take priority, then env vars
    try:
        cfg = st.secrets["postgres"]
        return psycopg2.connect(
            host=cfg["host"],
            port=int(cfg.get("port", 5432)),
            dbname=cfg["dbname"],
            user=cfg["user"],
            password=cfg["password"],
            sslmode=cfg.get("sslmode", "require"),
            connect_timeout=10,
        )
    except (KeyError, FileNotFoundError):
        return psycopg2.connect(
            host=os.environ["PG_HOST"],
            port=int(os.environ.get("PG_PORT", 5432)),
            dbname=os.environ.get("PG_DB", "olist"),
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
            sslmode="require",
            connect_timeout=10,
        )


@st.cache_data(ttl=3600)
def load_monthly_orders():
    conn = get_conn()
    df = pd.read_sql(
        """
        SELECT year_month,
               total_orders,
               total_revenue,
               avg_order_item_value,
               on_time_deliveries,
               on_time_rate
        FROM olist_prod.agg_monthly_orders
        ORDER BY year_month
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=3600)
def load_category_performance():
    conn = get_conn()
    df = pd.read_sql(
        """
        SELECT product_category_name_english,
               total_orders,
               total_revenue,
               avg_review_score
        FROM olist_prod.agg_category_performance
        ORDER BY total_revenue DESC
        LIMIT 20
        """,
        conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Tile 1 — Monthly Orders & Revenue
# ---------------------------------------------------------------------------
st.subheader("📅 Tile 1: Monthly Orders & Revenue")

try:
    monthly = load_monthly_orders()

    fig1 = go.Figure()
    fig1.add_trace(
        go.Bar(
            x=monthly["year_month"],
            y=monthly["total_orders"],
            name="Total Orders",
            marker_color="#4C72B0",
            yaxis="y1",
        )
    )
    fig1.add_trace(
        go.Scatter(
            x=monthly["year_month"],
            y=monthly["total_revenue"],
            name="Total Revenue (R$)",
            mode="lines+markers",
            line=dict(color="#DD8452", width=2),
            yaxis="y2",
        )
    )
    fig1.update_layout(
        xaxis=dict(title="Month", tickangle=-45),
        yaxis=dict(title="Orders", side="left"),
        yaxis2=dict(title="Revenue (R$)", side="right", overlaying="y"),
        legend=dict(x=0.01, y=0.99),
        height=420,
        hovermode="x unified",
    )
    st.plotly_chart(fig1, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Orders", f"{monthly['total_orders'].sum():,}")
    col2.metric("Total Revenue", f"R$ {monthly['total_revenue'].sum():,.0f}")
    col3.metric("Avg Item Value", f"R$ {monthly['avg_order_item_value'].mean():.2f}")

except Exception as e:
    st.error(f"Could not load monthly data: {e}")

st.divider()

# ---------------------------------------------------------------------------
# Tile 2 — Revenue by Product Category
# ---------------------------------------------------------------------------
st.subheader("📦 Tile 2: Top 20 Categories by Revenue")

try:
    cats = load_category_performance()

    fig2 = px.bar(
        cats.sort_values("total_revenue"),
        x="total_revenue",
        y="product_category_name_english",
        orientation="h",
        color="avg_review_score",
        color_continuous_scale="RdYlGn",
        labels={
            "total_revenue": "Total Revenue (R$)",
            "product_category_name_english": "Category",
            "avg_review_score": "Avg Review",
        },
        height=520,
    )
    fig2.update_layout(yaxis=dict(tickfont=dict(size=11)))
    st.plotly_chart(fig2, use_container_width=True)

    col1, col2 = st.columns(2)
    col1.metric("Top Category", cats.iloc[0]["product_category_name_english"])
    col2.metric("Top Revenue", f"R$ {cats.iloc[0]['total_revenue']:,.0f}")

except Exception as e:
    st.error(f"Could not load category data: {e}")

st.divider()
st.caption("Built with Streamlit + Plotly · Data warehouse: AWS RDS PostgreSQL · Transformations: dbt")
