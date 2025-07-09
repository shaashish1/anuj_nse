import os
import time
import sqlite3
import pandas as pd
import requests
from datetime import datetime

class NSEDataFetcher:
    def __init__(self):
        self.data_sources = {
            "BANKNIFTY": "https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY",
            "NIFTY": "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
        }
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        self.db_path = "data.db"
        self.conn = sqlite3.connect(self.db_path)
        self.create_table()

    def create_table(self):
        self.conn.execute("DROP TABLE IF EXISTS nse_data")
        query = """
        CREATE TABLE IF NOT EXISTS nse_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            NoOfContracts INTEGER,
            StrikePrice REAL,
            ExpDate TEXT,
            OptionType TEXT,
            POpen REAL,
            OICombinedFut REAL,
            FiveDayAvgVol REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.conn.execute(query)
        self.conn.commit()

    def fetch_data(self, url):
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to fetch data from {url}. HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def process_data(self, data):
        rows = []
        records = data.get("records", {}).get("data", [])
        timestamp = data.get("records", {}).get("timestamp", None)
        for record in records:
            strike_price = record.get("strikePrice")
            expiry_date = record.get("expiryDate")
            for option_type, option_data in {"CE": record.get("CE", {}), "PE": record.get("PE", {})}.items():
                row = [
                    option_data.get("underlying", None),  # Ticker
                    "NSE",  # XCH
                    option_data.get("lastPrice", None),  # LTP
                    option_data.get("totalTradedVolume", None),  # Qty
                    option_data.get("change", None),  # Chg
                    option_data.get("pChange", None),  # % Chg
                    option_data.get("bidQty", None),  # Bid Qty
                    option_data.get("bidprice", None),  # Bid
                    option_data.get("openPrice", None),  # Open
                    option_data.get("prevClose", None),  # P.Close
                    option_data.get("lowPrice", None),  # Low
                    option_data.get("highPrice", None),  # High
                    option_data.get("vwap", None),  # Avg Price
                    option_data.get("totalTradedVolume", None),  # T.Volume
                    option_data.get("totalValue", None),  # Total Value
                    option_data.get("openInterest", None),  # OI
                    option_data.get("numberOfContractsTraded", None),  # No.of contracts
                    strike_price,  # Strike Price
                    expiry_date,  # Exp. Date
                    option_type,  # Option Type
                    option_data.get("openPrice", None),  # P.Open
                    option_data.get("combinedOI", None),  # OI-Combined Fut
                    option_data.get("5DayAvgVol", None),  # 5-Days Avg Vol
                ]
                rows.append(row)
        return pd.DataFrame(rows, columns=[
            "Ticker", "XCH", "LTP", "Qty", "Chg", "PercChg", "BidQty", "Bid", "Open", "PClose", "Low", "High",
            "AvgPrice", "TVolume", "TotalValue", "OI", "NoOfContracts", "StrikePrice", "ExpDate", "OptionType",
            "POpen", "OICombinedFut", "FiveDayAvgVol"
        ])

    def save_to_db(self, data):
        data.to_sql('nse_data', self.conn, if_exists='append', index=False)
        print("Data saved to database.")

    def run(self):
        while True:
            for symbol, url in self.data_sources.items():
                print(f"Fetching data for {symbol}...")
                data = self.fetch_data(url)
                if data:
                    processed_data = self.process_data(data)
                    if not processed_data.empty:
                        self.save_to_db(processed_data)
                    else:
                        print(f"No tabular data available for {symbol}.")
                else:
                    print(f"Failed to fetch data for {symbol}.")
            print("Waiting 60 seconds before the next fetch...")
            time.sleep(60)

if __name__ == "__main__":
    fetcher = NSEDataFetcher()
    fetcher.run()
