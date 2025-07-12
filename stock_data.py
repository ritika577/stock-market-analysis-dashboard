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

    for ticker in tickers_list:
        try:
            print(f"Fetching data for {ticker}...")
            
            # First attempt with 15m interval
            try:
                df = yf.download(
                    tickers=ticker,
                    period='5d',
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
                        period='5d',
                        interval='1h',
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
                'Open': round(float(latest['Open']), 2),
                'High': round(float(latest['High']), 2),
                'Low': round(float(latest['Low']), 2),
                'Close': round(float(latest['Close']), 2),
                'Volume': int(latest['Volume'])
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



# --- Push to Google Sheet with cleanup ---
def update_google_sheet(df):
    print("\nUpdating Google Sheet...")
    print(f"Data to update: {len(df)} rows")

    service = get_gsheet_service()
    sheet = service.spreadsheets()

    # Get existing data
    print("Fetching existing data from sheet...")
    try:
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1:Z10000"
        ).execute()
        values = result.get('values', [])
    except Exception as e:
        print(f"✗ Failed to fetch sheet data: {e}")
        return

    headers = df.columns.tolist()
    existing_data = values[1:] if values else []
    final_rows = []
    removed_old = 0
    removed_na = 0

    cutoff = datetime.now() - timedelta(days=7)

    # Process existing data
    print(f"Processing {len(existing_data)} existing rows...")
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
                print(f"! Error parsing row: {e}")
    else:
        print("🆕 No existing data. Starting fresh.")

    # Clean new data
    print("Cleaning new data...")
    try:
        df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
        df = df.dropna(subset=['Datetime'])
        df = df[df['Datetime'] >= cutoff]
        df = df[~df[['Open', 'High', 'Low', 'Close', 'Volume']].isin(['N/A']).any(axis=1)]
    except Exception as e:
        print(f"✗ Error cleaning data: {e}")
        return

    if df.empty and not final_rows:
        print("! No valid data to update")
        return

    # Prepare final data
    new_data_rows = df.astype(str).values.tolist()
    final_data = [headers] + final_rows + new_data_rows

    # Update sheet
    print("\nUpdating sheet...")
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

        print("\n✓ Update complete!")
        print(f"• {len(final_rows)} existing rows kept")
        print(f"• {len(new_data_rows)} new rows added")
        print(f"• {removed_old} old rows removed")
        print(f"• {removed_na} invalid rows skipped")
    except Exception as e:
        print(f"✗ Failed to update sheet: {e}")


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

    if not df.empty:
        update_google_sheet(df)
    else:
        print("❌ No valid data to upload.")
