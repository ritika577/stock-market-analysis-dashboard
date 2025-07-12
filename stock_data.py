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

    # Step 1: Get current data from Google Sheet
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1:Z10000"
    ).execute()

    values = result.get('values', [])
    cleaned_data = []

    # Step 2: Determine header and clean old data
    if values:
        headers = values[0]
        existing_data = values[1:]

        try:
            datetime_index = headers.index('Datetime')
        except ValueError:
            print("❌ 'Datetime' column not found in existing sheet.")
            return

        cutoff = datetime.now() - timedelta(days=7)

        for row in existing_data:
            try:
                if len(row) <= datetime_index:
                    continue
                row_date = dateutil.parser.parse(row[datetime_index])
                if row_date >= cutoff and all(cell != 'N/A' for cell in row):
                    cleaned_data.append(row)
                else:
                    print(f"🗑️ Removed (invalid or old): {row}")
            except Exception as e:
                print(f"❌ Parse error for row {row}: {e}")

    else:
        headers = df.columns.tolist()

    # Step 3: Clean current DataFrame (new data)
    try:
        df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
        df = df.dropna(subset=['Datetime'])  # remove rows with bad datetime
        df = df[df['Datetime'] >= datetime.now() - timedelta(days=7)]
    except Exception as e:
        print(f"❌ Failed cleaning new data: {e}")
        return

    if df.empty and not cleaned_data:
        print("⚠️ Nothing valid to update.")
        return

    # Step 4: Format data for Google Sheets (strings)
    df = df.astype(str)
    new_rows = df.values.tolist()
    final_data = [headers] + cleaned_data + new_rows

    # Step 5: Clear old and upload new
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

    print(f"✅ Updated: {len(cleaned_data)} old rows kept, {len(new_rows)} new rows added.")




# --- MAIN ---
if __name__ == "__main__":
    df = fetch_stock_data()
    print(df)

    if not df.empty:
        update_google_sheet(df)
    else:
        print("❌ No valid data to upload.")
