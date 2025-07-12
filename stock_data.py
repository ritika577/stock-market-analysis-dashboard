import yfinance as yf
import pandas as pd
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
    df = tables[0]
    df = df[['Symbol', 'Security']]
    df.columns = ['Ticker', 'Company']
    df['Ticker'] = df['Ticker'].apply(lambda x: x.replace('.', '-'))
    return df.head(limit)

# --- Google Sheets Config ---
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

        time.sleep(5)

    return pd.DataFrame(results)

# --- Push to Google Sheet with old data removal ---
def update_google_sheet(df):
    print("📤 Cleaning & updating Google Sheet...")
    service = get_gsheet_service()
    sheet = service.spreadsheets()

    # Step 1: Read current sheet
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1:Z10000"
    ).execute()

    values = result.get('values', [])
    if not values:
        print("🆕 Sheet is empty. Adding headers and data.")
        final_data = [df.columns.tolist()] + df.values.tolist()
    else:
        headers = values[0]
        existing_data = values[1:]

        # Identify 'Datetime' column
        try:
            datetime_index = headers.index('Datetime')
        except ValueError:
            print("❌ 'Datetime' column not found.")
            return

        # Filter out old rows (> 30 days)
        cutoff_date = datetime.now() - timedelta(days=7)
        cleaned_data = []

        for row in existing_data:
            try:
                if len(row) <= datetime_index:
                    continue
                row_date = dateutil.parser.parse(row[datetime_index])
                if row_date >= cutoff_date:
                    cleaned_data.append(row)
                else:
                    print(f"🗑️ Skipped (old): {row}")
            except Exception as e:
                print(f"❌ Parse error: {row} → {e}")

        new_data = df.values.tolist()
        final_data = [headers] + cleaned_data + new_data

    # ✅ Step 2: Clear entire sheet
    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1:Z10000"
    ).execute()

    # ✅ Step 3: Write cleaned + new data
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption='RAW',
        body={'values': final_data}
    ).execute()

    print(f"✅ Sheet updated: {len(cleaned_data)} rows kept, {len(df)} new rows added.")





# --- MAIN ---
if __name__ == "__main__":
    df = fetch_stock_data()
    print(df)

    if not df.empty:
        update_google_sheet(df)
    else:
        print("❌ No valid data to upload.")
