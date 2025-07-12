# import pandas as pd 
# import streamlit as st 
# import plotly.express as px

# sheet_url = "https://docs.google.com/spreadsheets/d/1mZATyrCQsWqMf-Cc_Zfdr73PRbbhGRMFRw3tr9_u7MA/export?format=csv"
# df = pd.read_csv(sheet_url)

# # --- Step 2: Streamlit Page Title ---
# st.title("📈 Live Stock Market Dashboard")
# # Full list of companies


# # Dropdown with filtered or full list
# selected_company = st.selectbox(
#     "🔍 Select a company",
#     sorted(df['Company'].unique())  # Optional: sort alphabetically
# )



# # --- Step 4: Filter data ---
# filtered_df = df[df['Company'] == selected_company]
# st.markdown(f"### Ticker Symbol: `{filtered_df['Ticker'].values[0]}`")


# # --- Step 5: Line Chart for Closing Price ---
# st.subheader(f"Closing Price for {selected_company}")
# fig_close = px.line(
#     filtered_df,
#     x='Datetime',
#     y='Close',
#     title='Closing Price Over Time',
#     markers=True,  # Add dots to make points visible
#     labels={'Close': 'Closing Price', 'Datetime': 'Date & Time'}
# )
# fig_close.update_traces(mode='lines+markers')
# fig_close.update_layout(hovermode='x unified')
# st.plotly_chart(fig_close, use_container_width=True)


# # --- Step 6: Bar Chart for Volume ---
# st.subheader("Volume Traded")
# fig_volume = px.bar(
#     filtered_df,
#     x='Datetime',
#     y='Volume',
#     title='Volume Over Time',
#     color_discrete_sequence=['#1E90FF'],
#     labels={'Volume': 'Shares Traded', 'Datetime': 'Date & Time'}
# )
# fig_volume.update_layout(hovermode='x unified')
# st.plotly_chart(fig_volume, use_container_width=True)


# # --- Step 7: Option to Show Raw Data ---
# if st.checkbox("Show raw data"):
#     st.dataframe(filtered_df)

# st.markdown("### Summary Stats")
# col1, col2, col3 = st.columns(3)
# col1.metric("Open", f"${filtered_df['Open'].values[0]:,.2f}")
# col2.metric("High", f"${filtered_df['High'].values[0]:,.2f}")
# col3.metric("Low", f"${filtered_df['Low'].values[0]:,.2f}")

# limit = st.slider("How many recent data points to display?", min_value=1, max_value=len(filtered_df), value=5)
# latest_data = filtered_df.tail(limit)

# fig_close = px.line(latest_data, x='Datetime', y='Close', title='Recent Closing Prices', markers=True,labels={'Datetime': 'Date & Time'})
# st.plotly_chart(fig_close, use_container_width=True)


# import plotly.graph_objects as go

# fig_candle = go.Figure(data=[go.Candlestick(
#     x=filtered_df['Datetime'],
#     open=filtered_df['Open'],
#     high=filtered_df['High'],
#     low=filtered_df['Low'],
#     close=filtered_df['Close']
# )])
# fig_candle.update_layout(
#     title='📉 Candlestick Chart',
#     xaxis_title='Date & Time',  # ✅ This updates the label
#     yaxis_title='Stock Price',
#     xaxis_rangeslider_visible=False  # Optional: hides the zoom slider below the chart
# )

# st.plotly_chart(fig_candle, use_container_width=True)

# open_price = filtered_df['Open'].values[0]
# close_price = filtered_df['Close'].values[0]
# change = close_price - open_price
# direction = "🔺" if change > 0 else "🔻"

# st.metric("Change", f"{direction} ${abs(change):.2f}", delta=f"{(change/open_price)*100:.2f}%")

# st.download_button(
#     label="Download filtered data as CSV",
#     data=filtered_df.to_csv(index=False).encode('utf-8'),
#     file_name=f"{selected_company}_stock_data.csv",
#     mime='text/csv'
# )

# fig_close = px.line(
#     filtered_df,
#     x='Datetime',
#     y='Close',
#     markers=True,
#     title="Close Price with Hover Info",
#     hover_data=['Open', 'High', 'Low', 'Volume']
# )
# st.plotly_chart(fig_close, use_container_width=True)



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
