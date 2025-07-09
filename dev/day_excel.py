import os
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time


class NSEDataFetcher:
    def __init__(self):
        self.data_sources = {
            "BANKNIFTY_FUTURES": "https://www.nseindia.com/api/quote-derivative?symbol=BANKNIFTY",
            "NIFTY_FUTURES": "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY",
        }

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        }

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
        """Fetch data from the API."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error: Received status code {response.status_code} for URL: {url}")
                return None
        except Exception as e:
            print(f"Error fetching data: {e}")
            return None

    def filter_expiry_date(self, rows, expiry_date):
        """Filter rows based on the upcoming expiry date."""
        return [row for row in rows if row[19] == expiry_date]

    def process_data(self, data, key, expiry_date):
        """Process raw data into rows."""
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
        return self.filter_expiry_date(rows, expiry_date)

    def save_to_excel(self, rows, key, day_name):
        """Save data to Excel."""
        if not rows:
            print(f"No data to save for {key}. Skipping Excel creation.")
            return

        df = pd.DataFrame(rows, columns=self.columns).sort_values(by="Strike Price")
        file_path = os.path.join(self.output_dir, f"{day_name}_{key}.xlsx")
        df.to_excel(file_path, index=False)
        print(f"Saved: {file_path}")

    def save_to_db(self, rows):
        """Save data to SQLite."""
        if not rows:
            print("No data to save to the database. Skipping insertion.")
            return

        with self.conn:
            self.conn.executemany("""
                INSERT INTO nse_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
        print("Data saved to database.")

    def is_trading_hour(self):
        """Check if the current time is within trading hours (9:15 AM - 3:30 PM, Monday - Thursday)."""
        now = datetime.now()
        start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
        end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        return start_time <= now <= end_time and now.weekday() <= 3

    def run(self):
        """Main function to fetch, process, and save data."""
        while True:
            if self.is_trading_hour():
                for key, url in self.data_sources.items():
                    data = self.fetch_data(url)
                    if data:
                        expiry_date = (datetime.now() + timedelta((3 - datetime.now().weekday()) % 7)).strftime("%Y-%m-%d")
                        rows = self.process_data(data, key, expiry_date)
                        current_day = datetime.now().strftime("%A")
                        self.save_to_excel(rows, key, current_day)
                        self.save_to_db(rows)
                    else:
                        print(f"No data fetched for {key}.")
            else:
                print("The script is running outside working hours. Waiting for trading hours to start.")
            time.sleep(50)


if __name__ == "__main__":
    fetcher = NSEDataFetcher()
    fetcher.run()
