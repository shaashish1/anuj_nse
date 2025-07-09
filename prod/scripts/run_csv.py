import os
import requests
import pandas as pd
from datetime import datetime

class NSEDataFetcher:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
        }
        self.session = requests.Session()
        self.session.get('https://www.nseindia.com', headers=self.headers)
        self.data_sources = {
            "BANKNIFTY_FUTURES": "https://www.nseindia.com/api/quote-derivative?symbol=BANKNIFTY",
            "NIFTY_FUTURES": "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY",
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

    def fetch_data(self, url):
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

    def process_data(self, data, expiry_date):
        rows = []
        for item in data.get("stocks", []):
            if item["metadata"]["expiryDate"] != expiry_date:
                continue

            strike_price = float(item["metadata"].get("strikePrice") or 0)
            if strike_price == 0:
                # Skip rows where Strike Price is zero
                continue

            avg_price = float(item["marketDeptOrderBook"]["tradeInfo"].get("vmap", 0) or 0)
            t_volume = int(item["marketDeptOrderBook"]["tradeInfo"].get("tradedVolume", 0) or 0)
            calc_col_1 = (avg_price * t_volume) / 10**7

            rows.append([
                item["metadata"].get("instrumentType", ""),
                "NSE",
                float(item["metadata"].get("lastPrice") or 0),
                t_volume,
                float(item["metadata"].get("change") or 0),
                float(item["metadata"].get("pChange") or 0),
                int(item["marketDeptOrderBook"]["bid"][0].get("quantity", 0) or 0) if "bid" in item["marketDeptOrderBook"] else 0,
                float(item["marketDeptOrderBook"]["bid"][0].get("price", 0) or 0) if "bid" in item["marketDeptOrderBook"] else 0,
                float(item["metadata"].get("openPrice") or 0),
                float(item["metadata"].get("prevClose") or 0),
                float(item["metadata"].get("lowPrice") or 0),
                float(item["metadata"].get("highPrice") or 0),
                avg_price,
                t_volume,
                float(item["marketDeptOrderBook"]["tradeInfo"].get("value") or 0),
                int(item["marketDeptOrderBook"]["tradeInfo"].get("openInterest", 0) or 0),
                int(item["marketDeptOrderBook"]["tradeInfo"].get("changeinOpenInterest", 0) or 0),
                int(item["metadata"].get("numberOfContractsTraded", 0) or 0),
                strike_price,  # Use the parsed strike price
                item["metadata"]["expiryDate"],
                item["metadata"]["optionType"],
                float(item["metadata"].get("openPrice") or 0),
                float(item["marketDeptOrderBook"]["otherInfo"].get("combinedOI") or 0),
                float(item["marketDeptOrderBook"]["tradeInfo"].get("5DayAvgVol") or 0),
                calc_col_1,
            ])

        # Convert rows to DataFrame
        df = pd.DataFrame(rows, columns=self.columns)

        # Sort by 'Strike Price' and 'Option Type' to group 'CE' and 'PE' together
        df_sorted = df.sort_values(by=['Strike Price', 'Option Type'], ascending=[True, True])

        return df_sorted

    def save_to_csv(self, df, key):
        if df.empty:
            print(f"No data to save for {key}.")
            return

        current_day = datetime.now().strftime("%A")
        file_path = os.path.join(self.output_dir, f"{current_day}_{key}.csv")

        df.to_csv(file_path, index=False)
        print(f"Saved: {file_path}")

    def run(self):
        for key, url in self.data_sources.items():
            print(f"Fetching data for {key}...")
            raw_data = self.fetch_data(url)
            if raw_data:
                expiry_dates = sorted(
                    {item["metadata"]["expiryDate"] for item in raw_data.get("stocks", [])},
                    key=lambda date: datetime.strptime(date, "%d-%b-%Y")
                )
                if expiry_dates:
                    expiry_date = expiry_dates[0]
                    print(f"Processing data for expiry date: {expiry_date}")
                    df_sorted = self.process_data(raw_data, expiry_date)
                    self.save_to_csv(df_sorted, key)
                else:
                    print(f"No valid expiry dates found for {key}.")
            else:
                print(f"No data fetched for {key}.")

if __name__ == "__main__":
    fetcher = NSEDataFetcher()
    fetcher.run()
