import os
import json
import yfinance as yf
import pandas as pd
import base64
import time
from datetime import datetime, timedelta
from itertools import islice
from google.oauth2 import service_account
from googleapiclient.discovery import build
import dateutil.parser

# --- Get S&P 500 companies from Wikipedia ---
def get_sp500_companies(limit=100):
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0][['Symbol', 'Security']]
    df.columns = ['Ticker', 'Company']
    df['Ticker'] = df['Ticker'].str.replace('.', '-', regex=False)
    return df.head(limit)

# --- Google Sheets Config ---
SPREADSHEET_ID = '1mZATyrCQsWqMf-Cc_Zfdr73PRbbhGRMFRw3tr9_u7MA'
SHEET_NAME = 'Live Stock Data'
CREDENTIALS_FILE = 'credentials.json'

# --- Authenticate with Google Sheets ---
def get_gsheet_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    raw_json = os.getenv("GSHEET_CREDS_JSON")

    if not raw_json:
        raise RuntimeError("❌ Environment variable 'GSHEET_CREDS_JSON' is not set.")

    try:
        creds_dict = json.loads(raw_json)
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return build('sheets', 'v4', credentials=creds)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to parse credentials: {e}")

# --- Utility to batch tickers ---
def chunked(iterable, size):
    it = iter(iterable)
    return iter(lambda: list(islice(it, size)), [])

# --- Fetch latest stock data ---
def fetch_stock_data():
    companies_df = get_sp500_companies(500)
    tickers_list = companies_df['Ticker'].tolist()
    ticker_to_company = dict(zip(companies_df['Ticker'], companies_df['Company']))

    results = []
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for ticker in tickers_list:
        try:
            print(f"Fetching data for {ticker}...")
            
            # First attempt with 15m interval
            try:
                df = yf.download(
                    tickers=ticker,
                    period='1d',
                    interval='15m',
                    progress=False,
                    threads=False
                )
                
                if not df.empty:
                    df = df.dropna()
                    print(f"✓ Fetched 15m data for {ticker} ({len(df)} rows)")
                else:
                    print(f"! No 15m data for {ticker}, trying 1h...")
                    raise Exception("No 15m data")
                    
            except Exception as e:
                # Fallback to 1h interval
                try:
                    df = yf.download(
                        tickers=ticker,
                        period='1d',
                        interval='15m',
                        progress=False,
                        threads=False
                    )
                    
                    if not df.empty:
                        df = df.dropna()
                        print(f"✓ Fetched 1h data for {ticker} ({len(df)} rows)")
                    else:
                        raise Exception("No data available")
                        
                except Exception as inner_e:
                    print(f"✗ Failed to fetch {ticker}: {str(inner_e)}")
                    raise

            latest = df.iloc[-1]

            results.append({
                'Ticker': ticker,
                'Company': ticker_to_company[ticker],
                'Datetime': now,
                'Open': round(float(latest['Open'].iloc[0]), 2) if not latest['Open'].empty else 0.0,
                'High': round(float(latest['High'].iloc[0]), 2) if not latest['High'].empty else 0.0,
                'Low': round(float(latest['Low'].iloc[0]), 2) if not latest['Low'].empty else 0.0,
                'Close': round(float(latest['Close'].iloc[0]), 2) if not latest['Close'].empty else 0.0,
                'Volume': int(latest['Volume'].iloc[0]) if not latest['Volume'].empty else 0
            })

        except Exception as e:
            print(f"✗ Error fetching {ticker}: {e}")
            results.append({
                'Ticker': ticker,
                'Company': ticker_to_company.get(ticker, 'Unknown'),
                'Datetime': now,
                'Open': 'N/A',
                'High': 'N/A',
                'Low': 'N/A',
                'Close': 'N/A',
                'Volume': 'N/A'
            })

        time.sleep(0.5)  # reduce load

    return pd.DataFrame(results)

def clean_data_for_sheets(value):
    """Convert data types to be compatible with Google Sheets"""
    if pd.isna(value) or value == 'N/A':
        return ''
    if isinstance(value, (int, float)):
        return float(value)
    return str(value)

# --- Push to Google Sheet with cleanup ---
def update_google_sheet(df):
    print("\n📝 Updating Google Sheet...")
    print(f"📦 New data to upload: {len(df)} rows")

    try:
        # Clean the DataFrame
        df = df.copy()
        df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')

        # 👇 Cutoff set to 1 minute ago (for testing purposes)
        cutoff = datetime.now() - timedelta(days=7)
        print(f"⏱️ Cutoff time: {cutoff}")

        df = df[df['Datetime'] >= cutoff]
        if df.empty:
            print("⚠️ No new data after applying cutoff.")
            return False

        # Clean data for Sheets
        for col in df.columns:
            if col != 'Datetime':
                df[col] = df[col].apply(clean_data_for_sheets)

        df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # --- Access Google Sheets ---
        service = get_gsheet_service()
        sheet = service.spreadsheets()

        print("📄 Fetching existing sheet data...")
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:H"
        ).execute()

        existing_values = result.get('values', [])
        if len(existing_values) > 1:
            headers = existing_values[0]
            existing_df = pd.DataFrame(existing_values[1:], columns=headers)
            existing_df['Datetime'] = pd.to_datetime(existing_df['Datetime'], errors='coerce')

            before_count = len(existing_df)
            existing_df = existing_df[existing_df['Datetime'] >= cutoff]
            after_count = len(existing_df)
            print(f"🧹 Removed {before_count - after_count} old rows from sheet")

            # Format existing datetime to match new df
            existing_df['Datetime'] = existing_df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            combined_df = pd.concat([existing_df, df]).drop_duplicates(
                subset=['Ticker', 'Datetime'],
                keep='last'
            )
        else:
            combined_df = df

        values = [combined_df.columns.tolist()] + combined_df.values.tolist()

        print("⬇️ Clearing existing sheet data...")
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:Z"
        ).execute()

        print(f"⬆️ Uploading {len(values) - 1} rows...")
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption='USER_ENTERED',
            body={'values': values}
        ).execute()

        print(f"✅ Google Sheet updated successfully!")
        return True

    except Exception as e:
        print(f"❌ Error updating Google Sheet: {e}")
        if hasattr(e, 'content'):
            try:
                print(f"Error details: {e.content.decode('utf-8')}")
            except:
                print("⚠️ Could not decode error details")
        return False





# --- MAIN ---
if __name__ == "__main__":
    print("=== Stock Data Updater ===\n")
    start_time = time.time()
    
    try:
        df = fetch_stock_data()
        print(f"\nFetched data for {len(df)} tickers")
        
        if not df.empty:
            update_google_sheet(df)
        else:
            print("! No data to update")
            
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
    
    print(f"\nDone in {time.time() - start_time:.1f} seconds")