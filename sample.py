import streamlit as st
import pandas as pd
import plotly.express as px

# Title
st.title("📊 PhonePe Pulse - Transaction Dashboard")

# Year and Month selector
year = st.selectbox("Select Year", [2021, 2022, 2023, 2024])
month = st.selectbox("Select Month", list(range(1, 13)))

# Load data from your cleaned dataset (previously extracted from GitHub or DB)
df = pd.read_csv("transaction_data.csv")  # Or load from your SQL table

# Filter data based on selection
filtered_df = df[(df["Year"] == year) & (df["Month"] == month)]

# Show KPIs
total_txn = filtered_df["Transaction_Count"].sum()
total_value = filtered_df["Transaction_Amount"].sum()

col1, col2 = st.columns(2)
col1.metric("Total Transactions", f"{total_txn:,}")
col2.metric("Transaction Value", f"₹{total_value:,.2f}")

# State-wise bar chart
st.subheader("State-wise Transactions")
fig = px.bar(filtered_df, x="State", y="Transaction_Count", color="Transaction_Amount")
st.plotly_chart(fig)

# Optional: Map view (state-level)
st.subheader("📍 Transaction Map")
# You can use pydeck or folium if you have latitude/longitude data

# Table
st.dataframe(filtered_df)
