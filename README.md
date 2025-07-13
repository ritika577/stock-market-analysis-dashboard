# 📈 Stock Market Analysis Dashboard

An interactive web dashboard for analyzing and visualizing stock market data using Streamlit and Yahoo Finance (yfinance). Track live prices, run technical analysis, and explore company fundamentals—all in one place. Perfect for investors, data enthusiasts, and finance learners!

---

## 🚀 Features

- **Live & Historical Price Data:** Real-time price updates and historical data via yfinance.
- **Advanced Technical Analysis:** See below for details on all supported indicators.
- **Performance Metrics:** Detailed statistics for better investment decisions.
- **Company Fundamentals:** Financial ratios, P/E, EPS, and basic financials.
- **Interactive Visuals:** Responsive charts, dark/light theme, customizable timeframes.
- **Easy Export:** Download price data and metrics as CSV for your own analysis.

---

## 📊 Technical Analysis

- **Moving Averages (SMA, EMA):** Overlay customizable Simple and Exponential Moving Averages (e.g., 50-day, 100-day) on price charts.  
- **MACD & Signal Line:** Visualize the MACD oscillator and its 9-period signal line to detect momentum shifts.  
- **RSI (Relative Strength Index):** Calculate and chart a 14-day RSI to spot overbought/oversold conditions.  
- **Bollinger Bands:** Display upper and lower bands around price data to illustrate volatility and range.  
- **Custom Time Frames:** Choose your own lookback periods for all indicators (e.g., 30-day EMA, 14-day RSI).

---

## 📈 Performance Metrics

- **Price Change & % Change:** View both absolute and percentage price changes over any custom period, benchmarked from your selected window.
- **Year-over-Year Returns:** Analyze annual growth rates to evaluate long-term performance.
- **52‑Week Highs & Lows:** Instantly spot the highest and lowest prices over the last 52 weeks.
- **Volume Trends & Volatility:** Visualize daily trading volume and rolling price volatility (standard deviation) for selected periods.

---

## 🌐 Live Demo

👉 [Try the Dashboard](https://stock-market-analysis-dashboard.streamlit.app/)

---

## 🛠️ Installation & Quick Start

1. **Clone the repository**
    ```bash
    git clone https://github.com/ritika577/stock-market-analysis-dashboard.git
    cd stock-market-analysis-dashboard
    ```

2. **Create and activate a virtual environment**
    ```bash
    python -m venv venv
    # Activate:
    # Windows:
    venv\Scripts\activate
    # Mac/Linux:
    source venv/bin/activate
    ```

3. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4. **Run the Streamlit app**
    ```bash
    streamlit run app.py
    ```
    The dashboard will open in your browser at [http://localhost:8501](http://localhost:8501).

---

## ⚡ Data Update Script

To fetch and update stock data (using cron or manually), run:
```bash
python stock_data.py
```

## 👤 Connect with Me

- [LinkedIn: Ritika Chauhan](https://www.linkedin.com/in/ritika-chauhan-1370a9211/)
