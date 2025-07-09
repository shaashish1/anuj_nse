import pandas as pd
from fyers_apiv3 import fyersModel
from access_token import access_token, client_id
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FyersOptionChain:
    """Class to handle Fyers Option Chain operations"""
    
    def __init__(self):
        """Initialize FyersOptionChain with API credentials"""
        self.client_id = client_id
        self.fyers = None
        self.initialize_session()

    def initialize_session(self):
        """Initialize Fyers API session using access token"""
        try:
            # Initialize the FyersModel directly with the access token
            self.fyers = fyersModel.FyersModel(
                client_id=self.client_id,
                token=access_token,
                is_async=False
            )
            logger.info("Successfully initialized Fyers session")

        except Exception as e:
            logger.error(f"Error initializing Fyers session: {str(e)}")
            raise

    def fetch_option_chain(self, symbols=None):
        """
        Fetch option chain data for given symbols
        
        Args:
            symbols (str or list): Single symbol or list of symbols
            
        Returns:
            dict: Option chain data if successful, None otherwise
        """
        try:
            if symbols is None:
                symbols = ["NSE:NIFTY2521323400CE"]  # Default symbol
            elif isinstance(symbols, str):
                symbols = [symbols]
            
            # Convert list to comma-separated string
            symbols_str = ",".join(symbols)
            
            # Fetch quotes for all symbols
            data = {
                "symbols": symbols_str
            }
            response = self.fyers.quotes(data)
            
            if response["s"] != "ok":
                logger.error(f"Error fetching option chain: {response}")
                return None
                
            logger.info(f"Successfully fetched option chain for {len(symbols)} symbols")
            return response["d"]
            
        except Exception as e:
            logger.error(f"Error fetching option chain: {str(e)}")
            return None

    def process_option_chain(self, data):
        """
        Process option chain data into a pandas DataFrame
        
        Args:
            data (dict): Raw option chain data
            
        Returns:
            pandas.DataFrame: Processed option chain data
        """
        try:
            if not isinstance(data, list):
                data = [data]
                
            options_list = []
            for quote in data:
                symbol = quote.get("symbol", "")
                # Extract strike price and option type from symbol
                try:
                    # Find the position of CE or PE in the symbol
                    if "CE" in symbol:
                        option_type = "CE"
                        strike_pos = symbol.find("CE")
                    elif "PE" in symbol:
                        option_type = "PE"
                        strike_pos = symbol.find("PE")
                    else:
                        option_type = ""
                        strike_pos = -1
                    
                    # Extract strike price if found
                    if strike_pos > 0:
                        # Look for numbers before CE/PE
                        strike_str = ''.join(filter(str.isdigit, symbol[:strike_pos]))
                        strike_price = int(strike_str[-5:]) if strike_str else 0  # Last 5 digits as strike price
                    else:
                        strike_price = 0
                        
                except Exception as e:
                    logger.warning(f"Error parsing symbol {symbol}: {str(e)}")
                    strike_price = 0
                    option_type = ""
                
                option_data = {
                    "Symbol": symbol,
                    "Strike": strike_price,
                    "Type": option_type,
                    "LTP": quote.get("ltp", 0),
                    "Change": quote.get("ch", 0),
                    "Change%": quote.get("chp", 0),
                    "Volume": quote.get("vol", 0),
                    "OI": quote.get("oi", 0),
                    "Bid Qty": quote.get("bid_qty", 0),
                    "Bid": quote.get("bid", 0),
                    "Ask": quote.get("ask", 0),
                    "Ask Qty": quote.get("ask_qty", 0),
                    "Open": quote.get("open_price", 0),
                    "High": quote.get("high_price", 0),
                    "Low": quote.get("low_price", 0),
                    "Prev Close": quote.get("prev_close_price", 0),
                    "Total Trades": quote.get("tot_trades", 0)
                }
                options_list.append(option_data)

            # Create DataFrame and sort by Strike Price and Option Type
            df = pd.DataFrame(options_list)
            if not df.empty:
                df = df.sort_values(['Strike', 'Type'])
            logger.info(f"Successfully processed {len(options_list)} options")
            return df

        except Exception as e:
            logger.error(f"Error processing option chain: {str(e)}")
            return None

    def export_data(self, df, format='csv'):
        """
        Export option chain data to a file
        
        Args:
            df (pandas.DataFrame): Processed option chain data
            format (str): Export format ('csv' or 'excel')
        
        Returns:
            str: Path to the exported file
        """
        try:
            if df is None or df.empty:
                logger.error("No data to export")
                return None

            # Create output directory if it doesn't exist
            output_dir = os.path.join(os.path.dirname(__file__), 'output')
            os.makedirs(output_dir, exist_ok=True)

            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if format.lower() == 'csv':
                filename = f"option_chain_{timestamp}.csv"
                filepath = os.path.join(output_dir, filename)
                df.to_csv(filepath, index=False)
            else:
                filename = f"option_chain_{timestamp}.xlsx"
                filepath = os.path.join(output_dir, filename)
                df.to_excel(filepath, index=False)

            logger.info(f"Successfully exported data to {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error exporting data: {str(e)}")
            return None

def main():
    """Main function to demonstrate usage"""
    try:
        # Initialize FyersOptionChain
        fyers_oc = FyersOptionChain()

        # Specify the symbol
        symbols = ["NSE:NIFTY2521323400CE"]
        
        # Fetch option chain data
        option_chain_data = fyers_oc.fetch_option_chain(symbols)

        if option_chain_data:
            # Process the data
            df_options = fyers_oc.process_option_chain(option_chain_data)
            
            if df_options is not None and not df_options.empty:
                # Display first few rows
                pd.set_option('display.max_columns', None)
                print("\nOption Chain Preview:")
                print(df_options)

                # Export data in both formats
                csv_path = fyers_oc.export_data(df_options, 'csv')
                excel_path = fyers_oc.export_data(df_options, 'excel')
                
                if csv_path and excel_path:
                    print(f"\nData exported successfully:")
                    print(f"CSV: {csv_path}")
                    print(f"Excel: {excel_path}")
            else:
                print("No option chain data available")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()
