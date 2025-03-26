import streamlit as st
import pandas as pd
import yfinance as yf
import psycopg2
import os
import time

# Get the DATABASE_URL from Railway or use local PostgreSQL
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/stocks")

# Function to connect to the database
def get_db_connection(retries=5, delay=3):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(DB_URL)
            return conn
        except psycopg2.OperationalError as e:
            print(f"Database connection failed (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)  # Wait before retrying
    raise Exception("Failed to connect to the database after multiple attempts.")

# Function to create table if it doesn’t exist
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        with open("create_table.sql", "r") as file:
            cursor.execute(file.read())
        conn.commit()
        print("✅ Database table created or already exists!")
    except Exception as e:
        print("❌ Error creating table:", e)
    finally:
        cursor.close()
        conn.close()

# Function to fetch stock data
def fetch_stock_data(symbol):
    stock = yf.Ticker(symbol)
    df = stock.history(period="6mo")  # Get last 6 months of data
    df.reset_index(inplace=True)
    df["symbol"] = symbol
    return df[["Date", "symbol", "Open", "High", "Low", "Close", "Volume"]]

# Function to store data in database
def store_data(df):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stock_data (date, symbol, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, symbol) DO NOTHING;
            """, (row["Date"], row["symbol"], row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]))
        conn.commit()
        print(f"✅ Data stored for {df['symbol'][0]}")
    except Exception as e:
        print("❌ Error storing data:", e)
    finally:
        cursor.close()
        conn.close()

# Function to check for moving average crossover
def moving_average_crossover(short_window, long_window):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DISTINCT symbol FROM stock_data;")
        symbols = [row[0] for row in cursor.fetchall()]
        
        crossover_stocks = []
        for symbol in symbols:
            df = pd.read_sql(f"SELECT date, close FROM stock_data WHERE symbol='{symbol}' ORDER BY date ASC", conn)
            df["SMA_Short"] = df["close"].rolling(window=short_window).mean()
            df["SMA_Long"] = df["close"].rolling(window=long_window).mean()
            
            if df.iloc[-2]["SMA_Short"] < df.iloc[-2]["SMA_Long"] and df.iloc[-1]["SMA_Short"] > df.iloc[-1]["SMA_Long"]:
                crossover_stocks.append(symbol)

        return crossover_stocks
    except Exception as e:
        print("❌ Error fetching crossover stocks:", e)
        return []
    finally:
        cursor.close()
        conn.close()

# Streamlit UI
st.title("📈 Stock Moving Average Crossover Detector")

# Button to download data
if st.button("Download Data"):
    create_table()
    
    try:
        symbols_df = pd.read_csv("symbols.csv")
        symbols = symbols_df.iloc[:, 0].tolist()  # Read first column as symbol list
        
        for symbol in symbols:
            df = fetch_stock_data(symbol)
            store_data(df)
        
        st.success("✅ Data downloaded and stored successfully!")
    except Exception as e:
        st.error(f"❌ Error downloading data: {e}")

# Moving average input
short_window = st.number_input("Enter Short Moving Average", min_value=5, max_value=100, value=20, step=1)
long_window = st.number_input("Enter Long Moving Average", min_value=10, max_value=200, value=50, step=1)

# Button to check crossovers
if st.button("Find Crossovers"):
    stocks = moving_average_crossover(short_window, long_window)
    if stocks:
        st.success("✅ Stocks with crossover found:")
        st.write(stocks)
    else:
        st.warning("⚠️ No crossover found.")
