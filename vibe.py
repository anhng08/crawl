import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide")
st.title("📊 Order Analytics Dashboard")

# ---------- UPLOAD ----------
file = st.file_uploader("Upload CSV", type=["csv"])

if file:
    df = pd.read_csv(file)

    df = df.replace(['--', 'N/A', 'na', ''], pd.NA)

    num_cols = ["Order Quantity", "Unit Price", "Line Total", "Total Unit Cost"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors='coerce')

    # ---------- KPI ----------
    total_revenue = df["Line Total"].sum()
    total_orders = df["OrderNumber"].nunique()
    total_quantity = df["Order Quantity"].sum()
    avg_order_value = total_revenue / total_orders if total_orders else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Revenue", f"{total_revenue:,.0f}")
    col2.metric("🧾 Orders", total_orders)
    col3.metric("📦 Quantity", total_quantity)
    col4.metric("💵 AOV", f"{avg_order_value:,.0f}")

    # ---------- FILTER ----------
    st.subheader("🎯 Filters")

    c1, c2, c3 = st.columns(3)

    with c1:
        channel_filter = st.multiselect("Channel", df["Channel"].dropna().unique())
    with c2:
        region_filter = st.multiselect("Region", df["Delivery Region Index"].dropna().unique())
    with c3:
        product_filter = st.multiselect("Product", df["Product Description Index"].dropna().unique())

    filtered_df = df.copy()

    if channel_filter:
        filtered_df = filtered_df[filtered_df["Channel"].isin(channel_filter)]
    if region_filter:
        filtered_df = filtered_df[filtered_df["Delivery Region Index"].isin(region_filter)]
    if product_filter:
        filtered_df = filtered_df[filtered_df["Product Description Index"].isin(product_filter)]

    # ---------- DATE FILTER ----------
    min_date = df["OrderDate"].min()
    max_date = df["OrderDate"].max()

    start_date, end_date = st.date_input("Date range", value=(min_date, max_date))

    filtered_df = filtered_df[
        (filtered_df["OrderDate"] >= pd.to_datetime(start_date)) &
        (filtered_df["OrderDate"] <= pd.to_datetime(end_date))
    ]

    # ---------- CHART DATA ----------
    rev_channel = filtered_df.groupby("Channel")["Line Total"].sum().reset_index()

    rev_time = (
        filtered_df
        .dropna(subset=["OrderDate"])
        .groupby(pd.Grouper(key="OrderDate", freq="D"))["Line Total"]
        .sum()
        .reset_index()
    )

    top_products = (
        filtered_df
        .groupby("Product Description Index")["Line Total"]
        .sum()
        .nlargest(10)
        .reset_index()
    )

    # ---------- CHARTS ----------
    st.subheader("📊 Charts")

    fig_channel = px.bar(rev_channel, x="Channel", y="Line Total", title="Revenue by Channel")
    fig_time = px.line(rev_time, x="OrderDate", y="Line Total", title="Revenue Over Time")
    fig_product = px.bar(top_products, x="Product Description Index", y="Line Total", title="Top Products")

    st.plotly_chart(fig_channel, use_container_width=True)
    st.plotly_chart(fig_time, use_container_width=True)
    st.plotly_chart(fig_product, use_container_width=True)

    # ---------- ADVANCED INSIGHTS ----------
    st.subheader("🧠 Advanced Insights")

    # 2. Pareto
    pareto = (
        filtered_df
        .groupby("Product Description Index")["Line Total"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    pareto["cum_%"] = pareto["Line Total"].cumsum() / pareto["Line Total"].sum()

    top_80 = pareto[pareto["cum_%"] <= 0.8]

    # 3. Channel Efficiency
    channel_eff = filtered_df.groupby("Channel").agg({
        "Line Total": "sum",
        "OrderNumber": "nunique"
    }).reset_index()

    channel_eff["AOV"] = channel_eff["Line Total"] / channel_eff["OrderNumber"]

    # 4. Risk
    top_channel_share = (
        rev_channel["Line Total"].max() / rev_channel["Line Total"].sum()
        if not rev_channel.empty else 0
    )

    # 5. Best day
    filtered_df["weekday"] = filtered_df["OrderDate"].dt.day_name()

    best_day = (
        filtered_df.groupby("weekday")["Line Total"]
        .sum()
        .sort_values(ascending=False)
    )

    # ---------- DISPLAY ----------
    c2, c3, c4 = st.columns(3)

    c2.metric("🏆 Top Channel Share", f"{top_channel_share:.2%}")
    c3.metric("📦 Products (80% rev)", len(top_80))
    c4.metric("📅 Best Day", best_day.index[0] if not best_day.empty else "N/A")

    st.markdown("---")

    # DETAILS
    colA, colB = st.columns(2)

    with colA:
        st.write("🔥 Channel Efficiency (AOV)")
        st.dataframe(channel_eff.sort_values("AOV", ascending=False))


    with colB:
        st.write("📊 Pareto (Top products)")
        st.dataframe(top_80)