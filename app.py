import streamlit as st
import pandas as pd
import yfinance as yf
import sqlite3

# Database setup
DB_FILE = "stocks.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stock_data (
                 symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, 
                 PRIMARY KEY (symbol, date))''')
    conn.commit()
    conn.close()

# Fetch stock data and store in database
def fetch_and_store_data(symbols):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    for symbol in symbols:
        try:
            data = yf.download(symbol, period="1y")  # 1 year of data
            data.reset_index(inplace=True)
            for _, row in data.iterrows():
                c.execute('''INSERT OR IGNORE INTO stock_data (symbol, date, open, high, low, close, volume)
                             VALUES (?, ?, ?, ?, ?, ?, ?)''',
                          (symbol, row['Date'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
        except Exception as e:
            st.error(f"Error fetching {symbol}: {e}")
    conn.commit()
    conn.close()

# Check moving average crossover
def find_crossover(symbols, short_window, long_window):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    crossed_stocks = []
    for symbol in symbols:
        df = pd.read_sql(f"SELECT date, close FROM stock_data WHERE symbol='{symbol}' ORDER BY date ASC", conn)
        if df.empty:
            continue
        df['Short_MA'] = df['close'].rolling(window=short_window).mean()
        df['Long_MA'] = df['close'].rolling(window=long_window).mean()
        
        if df.iloc[-1]['Short_MA'] > df.iloc[-1]['Long_MA'] and df.iloc[-2]['Short_MA'] <= df.iloc[-2]['Long_MA']:
            crossed_stocks.append(symbol)
    conn.close()
    return crossed_stocks

# Streamlit UI
st.title("Stock Moving Average Crossover Finder")
init_db()

# Load symbols from CSV
try:
    symbols_df = pd.read_csv("symbols.csv")
    symbols = symbols_df['Symbol'].tolist()
except Exception as e:
    st.error("Error loading symbols.csv: " + str(e))
    symbols = []

# Download data button
if st.button("Download Stock Data"):
    fetch_and_store_data(symbols)
    st.success("Stock data updated successfully!")

# Moving Average Input
a, b = st.number_input("Short MA Lookback", min_value=1, value=20), st.number_input("Long MA Lookback", min_value=1, value=50)
if st.button("Find Crossovers"):
    result = find_crossover(symbols, a, b)
    st.write("Stocks with crossover:", result)
