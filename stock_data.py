import yfinance as yf
import pandas as pd
import time
from datetime import datetime
from itertools import islice
from google.oauth2 import service_account
from googleapiclient.discovery import build

def get_sp500_companies(limit=500):
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]  # First table contains the data

    # Rename columns for clarity
    df = df[['Symbol', 'Security']]
    df.columns = ['Ticker', 'Company']

    # Replace BRK.B with BRK-B (for Yahoo Finance)
    df['Ticker'] = df['Ticker'].apply(lambda x: x.replace('.', '-'))

    # Limit to first `n` companies
    return df.head(limit)


SPREADSHEET_ID = '1mZATyrCQsWqMf-Cc_Zfdr73PRbbhGRMFRw3tr9_u7MA'
SHEET_NAME = 'Live Stock Data'
CREDENTIALS_FILE = 'credentials.json'

# --- Authenticate with Google Sheets ---
def get_gsheet_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return build('sheets', 'v4', credentials=creds)

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

    for batch in chunked(tickers_list, 25):
        try:
            data = yf.download(
                tickers=batch,
                period='5d',
                interval='15m',
                group_by='ticker',
                progress=False,
                threads=False
            )
        except Exception as e:
            print(f"❌ Batch error: {e}")
            continue

        for ticker in batch:
            try:
                df = data[ticker] if len(batch) > 1 else data
                df = df.dropna()
                if df.empty:
                    raise ValueError("No data received.")
                latest = df.iloc[-1]

                results.append({
                    'Ticker': ticker,
                    'Company': ticker_to_company[ticker],
                    'Datetime': now,
                    'Open': round(latest['Open'], 2),
                    'High': round(latest['High'], 2),
                    'Low': round(latest['Low'], 2),
                    'Close': round(latest['Close'], 2),
                    'Volume': int(latest['Volume'])
                })
            except Exception as e:
                print(f"⚠️ Error fetching {ticker}: {e}")
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

        time.sleep(5)  # Respect Yahoo Finance rate limit

    return pd.DataFrame(results)

# --- Push to Google Sheet ---
def update_google_sheet(df):
    print("\U0001F4E4 Updating Google Sheet...")
    service = get_gsheet_service()
    sheet = service.spreadsheets()

    clear_range = f"{SHEET_NAME}!A1:Z1000"   # No quotes in .clear()
    update_range = f"'{SHEET_NAME}'!A1"      # Quotes required in .update()

    # Clear existing
    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=clear_range
    ).execute()

    # Prepare data
    values = [df.columns.tolist()] + df.values.tolist()

    # Update sheet
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=update_range,
        valueInputOption='RAW',
        body={'values': values}
    ).execute()

    print("\u2705 Google Sheet updated successfully!")

# --- MAIN ---
if __name__ == "__main__":
    df = fetch_stock_data()
    print(df)

    if not df.empty:
        update_google_sheet(df)
    else:
        print("\u274C No valid data to upload.")
