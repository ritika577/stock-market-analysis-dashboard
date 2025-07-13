import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# -----------------------
# Page Configuration
# -----------------------
st.set_page_config(page_title="📈 Live Stock Market Dashboard", layout="wide")

# -----------------------
# Load Data (Google Sheet URL or local CSV)
# -----------------------
sheet_url = 'https://docs.google.com/spreadsheets/d/1mZATyrCQsWqMf-Cc_Zfdr73PRbbhGRMFRw3tr9_u7MA/export?format=csv'
df = pd.read_csv(sheet_url)

# -----------------------
# Data Cleaning
# -----------------------

# Standardize string representations of missing values
df.replace(
    to_replace=["NA", "N/A", "None", "NULL", "null", "", " "],
    value=pd.NA,
    inplace=True
)

# Drop rows where any essential column is missing
essential_cols = ['Ticker', 'Company', 'Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
df.dropna(subset=essential_cols, inplace=True)

# Convert datatypes where needed
df['Open'] = pd.to_numeric(df['Open'], errors='coerce')
df['High'] = pd.to_numeric(df['High'], errors='coerce')
df['Low'] = pd.to_numeric(df['Low'], errors='coerce')
df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')

# Re-drop if numeric conversion failed
df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)


# Convert datetime string to actual datetime object
df['Datetime'] = pd.to_datetime(df['Datetime'])

# -----------------------
# App Title
# -----------------------
st.markdown("## 📊 Live Stock Market Dashboard")

# -----------------------
# Company Dropdown with Search
# -----------------------
selected_company = st.selectbox(
    "🔍 Select a company", 
    sorted(df['Company'].unique())
)

# Filter Data by Selected Company
filtered_df = df[df['Company'] == selected_company].sort_values("Datetime")

# -----------------------
# Summary Stats
# -----------------------
st.markdown("### 🧾 Summary Stats")
col1, col2, col3 = st.columns(3)
col1.metric("Open", f"${filtered_df['Open'].values[-1]:,.2f}")
col2.metric("High", f"${filtered_df['High'].values[-1]:,.2f}")
col3.metric("Low", f"${filtered_df['Low'].values[-1]:,.2f}")

limit = st.slider("How many recent data points to display?", min_value=1, max_value=len(filtered_df), value=5)
latest_data = filtered_df.tail(limit)

fig_close = px.line(latest_data, x='Datetime', y='Close', title='Recent Closing Prices', markers=True, labels={'Datetime': 'Date & Time'})
st.plotly_chart(fig_close, use_container_width=True)

# -----------------------
# Line Chart – Closing Price
# -----------------------
st.markdown("### 📉 Closing Price Over Time")
fig_close = px.line(
    filtered_df,
    x="Datetime",
    y="Close",
    title=f"Closing Price for {selected_company}"
)
fig_close.update_traces(mode='lines+markers')
fig_close.update_layout(
    hovermode='x unified',
    xaxis_title="Date & Time",
    yaxis_title="Close"
)
st.plotly_chart(fig_close, use_container_width=True)

# -----------------------
# Bar Chart – Volume
# -----------------------
st.markdown("### 📦 Volume Traded Over Time")
fig_vol = px.bar(
    filtered_df,
    x="Datetime",
    y="Volume",
    title="Volume Over Time",
    color_discrete_sequence=["#7FDBFF"]
)
fig_vol.update_layout(
    xaxis_title="Date & Time",
    yaxis_title="Volume",
    hovermode="x unified"
)
st.plotly_chart(fig_vol, use_container_width=True)

# -----------------------
# Candlestick Chart
# -----------------------
st.markdown("### 🕯️ Candlestick Chart")
fig_candle = go.Figure(data=[go.Candlestick(
    x=filtered_df['Datetime'],
    open=filtered_df['Open'],
    high=filtered_df['High'],
    low=filtered_df['Low'],
    close=filtered_df['Close'],
    name="Price"
)])
fig_candle.update_layout(
    title=f"Candlestick Chart for {selected_company}",
    xaxis_title="Date & Time",
    yaxis_title="Price",
    xaxis_rangeslider_visible=False
)
st.plotly_chart(fig_candle, use_container_width=True)

open_price = filtered_df['Open'].values[0]
close_price = filtered_df['Close'].values[0]
change = close_price - open_price
direction = "🔺" if change > 0 else "🔻"

st.metric("Change", f"{direction} ${abs(change):.2f}", delta=f"{(change/open_price)*100:.2f}%")

st.download_button(
    label="Download filtered data as CSV",
    data=filtered_df.to_csv(index=False).encode('utf-8'),
    file_name=f"{selected_company}_stock_data.csv",
    mime='text/csv'
)


# -----------------------
# Show Raw Data (Optional)
# -----------------------
st.markdown("### 📂 Show Raw Data")
if st.checkbox("Show raw data"):
    st.dataframe(filtered_df.tail(10), use_container_width=True)
