import os
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time


class NSEDataFetcher:
    def __init__(self):
        """
        Initialize the class with session management and data sources.
        """
        try:
            # Initialize session and make an initial request to the NSE website
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
            }
            self.session = requests.Session()
            self.session.get('https://www.nseindia.com', headers=self.headers)  # Initialize session
            print("Session initialized successfully.")
        except Exception as e:
            print(f"Error initializing session: {e}")

        # Define the data sources
        self.data_sources = {
            "BANKNIFTY_FUTURES": "https://www.nseindia.com/api/quote-derivative?symbol=BANKNIFTY",
            "NIFTY_FUTURES": "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY",
        }

        # Columns for data processing
        self.columns = [
            "Ticker", "XCH", "LTP", "Qty.", "Chg", "% Chg", "Bid Qty", "Bid",
            "Open", "P.Close", "Low", "High", "Avg Price", "T.Volume",
            "Total Value", "OI", "OI Change", "No.of contracts", "Strike Price",
            "Exp. Date", "Option Type", "P.Open", "OI-Combined Fut", "5-Days Avg Vol",
            "Calc Column 1"
        ]

        self.output_dir = "output"
        os.makedirs(self.output_dir, exist_ok=True)

        # SQLite Database
        self.db_path = "data.db"
        self.conn = sqlite3.connect(self.db_path)
        self.initialize_db()

    def initialize_db(self):
        """Create the SQLite database schema."""
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nse_data (
                Ticker TEXT,
                XCH TEXT,
                LTP REAL,
                Qty INTEGER,
                Chg REAL,
                PercChg REAL,
                BidQty INTEGER,
                Bid REAL,
                Open REAL,
                PClose REAL,
                Low REAL,
                High REAL,
                AvgPrice REAL,
                TVolume INTEGER,
                TotalValue REAL,
                OI INTEGER,
                OIChange INTEGER,
                NoOfContracts INTEGER,
                StrikePrice REAL,
                ExpDate TEXT,
                OptionType TEXT,
                POpen REAL,
                OICombinedFut REAL,
                FiveDayAvgVol REAL,
                CalcColumn1 REAL
            )
            """)

    def fetch_data(self, url):
        """
        Fetch data from the specified NSE API URL using the initialized session.
        """
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error: Received status code {response.status_code} for URL: {url}")
                return None
        except Exception as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def process_data(self, data, key):
        """
        Process raw data into rows.
        """
        rows = []
        for item in data.get("stocks", []):
            identifier = item["metadata"].get("identifier", "")
            ticker = "NIFTY" if key == "NIFTY_FUTURES" else identifier.split(" ")[1] if " " in identifier else None

            avg_price = float(item["marketDeptOrderBook"]["tradeInfo"].get("vmap", 0) or 0)
            t_volume = int(item["marketDeptOrderBook"]["tradeInfo"].get("tradedVolume", 0) or 0)
            calc_col_1 = (avg_price * t_volume) / 10**7

            rows.append([ticker, "NSE", float(item["metadata"].get("lastPrice") or 0), t_volume,
                         float(item["metadata"].get("change") or 0), float(item["metadata"].get("pChange") or 0),
                         int(item["marketDeptOrderBook"]["bid"][0].get("quantity", 0) or 0) if "bid" in item["marketDeptOrderBook"] else 0,
                         float(item["marketDeptOrderBook"]["bid"][0].get("price", 0) or 0) if "bid" in item["marketDeptOrderBook"] else 0,
                         float(item["metadata"].get("openPrice") or 0), float(item["metadata"].get("prevClose") or 0),
                         float(item["metadata"].get("lowPrice") or 0), float(item["metadata"].get("highPrice") or 0),
                         avg_price, t_volume, float(item["marketDeptOrderBook"]["tradeInfo"].get("value") or 0),
                         int(item["marketDeptOrderBook"]["tradeInfo"].get("openInterest", 0) or 0),
                         int(item["marketDeptOrderBook"]["tradeInfo"].get("changeinOpenInterest", 0) or 0),
                         int(item["metadata"].get("numberOfContractsTraded", 0) or 0), float(item["metadata"].get("strikePrice") or 0),
                         item["metadata"].get("expiryDate", ""), item["metadata"].get("optionType", ""),
                         float(item["metadata"].get("openPrice") or 0), float(item["marketDeptOrderBook"]["otherInfo"].get("combinedOI") or 0),
                         float(item["marketDeptOrderBook"]["tradeInfo"].get("5DayAvgVol") or 0), calc_col_1])
        return rows

    def save_to_excel(self, rows, key):
        """
        Save processed rows to an Excel file by day of the week.
        """
        if not rows:
            print(f"No data to save for {key}.")
            return

        # Get the current day of the week (e.g., "Monday", "Tuesday", etc.)
        current_day = datetime.now().strftime("%A")
        file_path = os.path.join(self.output_dir, f"{current_day}_{key}.xlsx")

        df = pd.DataFrame(rows, columns=self.columns)
        df.to_excel(file_path, index=False)
        print(f"Saved: {file_path}")

    def cleanup_and_initialize(self):
        """
        Clean up and initialize default files every Friday at 9:15 AM.
        """
        current_day = datetime.now().strftime("%A")
        current_time = datetime.now().strftime("%H:%M")

        if current_day == "Friday" and current_time == "09:15":
            print("Performing weekly cleanup...")
            for day in ["Monday", "Tuesday", "Wednesday", "Thursday"]:
                for key in self.data_sources.keys():
                    file_path = os.path.join(self.output_dir, f"{day}_{key}.xlsx")
                    df = pd.DataFrame(columns=self.columns).fillna(0)
                    df.to_excel(file_path, index=False)
                    print(f"Initialized: {file_path}")

    def run(self):
        """
        Main function to fetch, process, and save data for each source.
        """
        while True:
            self.cleanup_and_initialize()

            for key, url in self.data_sources.items():
                print(f"Fetching data for {key}...")
                raw_data = self.fetch_data(url)
                if raw_data:
                    rows = self.process_data(raw_data, key)
                    self.save_to_excel(rows, key)
                else:
                    print(f"No data fetched for {key}.")

            time.sleep(60)


if __name__ == "__main__":
    fetcher = NSEDataFetcher()
    fetcher.run()
