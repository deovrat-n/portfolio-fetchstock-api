from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

app = FastAPI()

# Allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Allow your React app
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "spring",
    "port": 3306
}

NIFTY_50_SYMBOLS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "LT.NS", "AXISBANK.NS", "BAJFINANCE.NS", "HCLTECH.NS", "ASIANPAINT.NS",
    "MARUTI.NS", "SUNPHARMA.NS", "TITAN.NS", "ULTRACEMCO.NS", "ONGC.NS",
    "WIPRO.NS", "POWERGRID.NS", "NTPC.NS", "BAJAJFINSV.NS", "NESTLEIND.NS",
    "JSWSTEEL.NS", "TECHM.NS", "HDFCLIFE.NS", "INDUSINDBK.NS", "GRASIM.NS",
    "TATAMOTORS.NS", "ADANIENT.NS", "TATASTEEL.NS", "COALINDIA.NS", "SBILIFE.NS",
    "EICHERMOT.NS", "HINDALCO.NS", "DRREDDY.NS", "BPCL.NS", "CIPLA.NS",
    "DIVISLAB.NS", "APOLLOHOSP.NS", "BRITANNIA.NS", "HEROMOTOCO.NS", "UPL.NS",
    "ADANIPORTS.NS", "BAJAJ-AUTO.NS", "M&M.NS", "IOC.NS", "SHREECEM.NS"
]

def calculate_moving_averages(df, period=10):
    df["SMA"] = df["Close"].rolling(window=period).mean()
    df["EMA"] = df["Close"].ewm(span=period, adjust=False).mean()
    return df


# Function to insert/update Nifty 50 stocks in the database
def insert_or_update_nifty_50_stocks():

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for symbol in NIFTY_50_SYMBOLS:
            stock = yf.Ticker(symbol)
            name = stock.info.get("shortName", symbol)
            data = stock.history(period="1d")

            if not data.empty:
                latest_price = data["Close"].iloc[-1]  # Get latest closing price
            else:
                latest_price = None

            # Check if stock exists
            cursor.execute("SELECT COUNT(*) FROM stocks WHERE symbol = %s", (symbol,))
            exists = cursor.fetchone()[0]

            if exists > 0:
                # Update stock price if it exists
                cursor.execute("UPDATE stocks SET price = %s WHERE symbol = %s", (latest_price, symbol))
            else:
                # Insert new stock if it does not exist
                cursor.execute("INSERT INTO stocks (name, symbol, price) VALUES (%s, %s, %s)", (name, symbol, latest_price))

        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Nifty 50 stocks updated successfully"}
    except Error as e:
        return {"error": str(e)}


# API endpoint to insert/update Nifty 50 stocks
@app.get("/update-nifty50")
def update_nifty50_stocks():
    result = insert_or_update_nifty_50_stocks()
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@app.get("/historical/{symbol}")
def get_historical_data(
    symbol: str,
    period: str = Query("1M", description="Time period (1M, 3M, 6M, 1Y, 5Y, Max)"),
    moving_avg_period: int = Query(10, description="Moving average period"),
):
    try:
        today = datetime.today()
        period_mapping = {
            "1M": timedelta(days=30),
            "3M": timedelta(days=90),
            "6M": timedelta(days=180),
            "1Y": timedelta(days=365),
            "5Y": timedelta(days=5 * 365),
        }

        stock = yf.Ticker(symbol)
        full_history = stock.history(period="max")  # Fetch all available data

        if full_history.empty:
            raise HTTPException(status_code=404, detail="No historical data found")

        # Convert first available data index to naive datetime
        earliest_date = full_history.index[0].to_pydatetime().replace(tzinfo=None)

        # Determine the requested start date
        if period == "Max":
            start_date = earliest_date  # Use the earliest available data
        else:
            requested_start = today - period_mapping[period]
            start_date = max(requested_start, earliest_date)  # Avoid exceeding available data

        start_str = start_date.strftime('%Y-%m-%d')
        end_str = today.strftime('%Y-%m-%d')

        df = stock.history(start=start_str, end=end_str)

        if df.empty:
            raise HTTPException(status_code=404, detail="No data available for the selected period")

        df = calculate_moving_averages(df, moving_avg_period)

        return {
            "symbol": symbol,
            "data": df[["Close", "SMA", "EMA"]].replace({np.nan: None}).to_dict(orient="index")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
