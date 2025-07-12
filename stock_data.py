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

# --- Push to Google Sheet with cleanup ---
def update_google_sheet(df):
    print("📤 Starting Google Sheet update...")

    service = get_gsheet_service()
    sheet = service.spreadsheets()

    # --- Step 1: Get existing data ---
    try:
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1:Z10000"
        ).execute()
        values = result.get('values', [])
    except Exception as e:
        print(f"❌ Failed to fetch existing sheet data: {e}")
        return

    headers = df.columns.tolist()
    existing_data = values[1:] if values else []
    final_rows = []
    removed_old = 0
    removed_na = 0

    cutoff = datetime.now() - timedelta(days=7)

    # --- Step 2: Clean old existing data ---
    if values:
        try:
            headers = values[0]
            datetime_idx = headers.index('Datetime')
            price_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            price_indices = [headers.index(col) for col in price_cols]
        except ValueError as e:
            print(f"❌ Missing expected columns in header: {e}")
            return

        for row in existing_data:
            try:
                if len(row) <= datetime_idx:
                    continue
                row_date = dateutil.parser.parse(row[datetime_idx])
                if row_date < cutoff:
                    removed_old += 1
                    continue
                if any(row[i] == 'N/A' for i in price_indices if i < len(row)):
                    removed_na += 1
                    continue
                final_rows.append(row)
            except Exception as e:
                print(f"⚠️ Error parsing row: {row} → {e}")
    else:
        print("🆕 No existing data. Starting fresh.")

    # --- Step 3: Clean new data ---
    try:
        df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
        df = df.dropna(subset=['Datetime'])
        df = df[df['Datetime'] >= cutoff]
        df = df[~df[['Open', 'High', 'Low', 'Close', 'Volume']].isin(['N/A']).any(axis=1)]
    except Exception as e:
        print(f"❌ Error cleaning new data: {e}")
        return

    if df.empty and not final_rows:
        print("⚠️ No valid data to write. Skipping update.")
        return

    new_data_rows = df.astype(str).values.tolist()
    final_data = [headers] + final_rows + new_data_rows

    # --- Step 4: Clear and update sheet ---
    try:
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1:Z10000"
        ).execute()

        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption='RAW',
            body={'values': final_data}
        ).execute()

        print(f"✅ Sheet updated:")
        print(f"   • {len(final_rows)} valid old rows kept")
        print(f"   • {len(new_data_rows)} new rows added")
        print(f"   • {removed_old} old rows removed (>7 days)")
        print(f"   • {removed_na} rows with 'N/A' removed")
    except Exception as e:
        print(f"❌ Failed to update Google Sheet: {e}")


# --- MAIN ---
if __name__ == "__main__":
    df = fetch_stock_data()
    print(df)

    if not df.empty:
        update_google_sheet(df)
    else:
        print("❌ No valid data to upload.")
