import streamlit as st
import pandas as pd
import yfinance as yf
import sqlite3  # For SQLite (if needed)
import os

# Railway Database Connection (PostgreSQL)
import psycopg2
DATABASE_URL = os.getenv("DATABASE_URL")  # Railway auto-sets this

def get_db_connection():
    if DATABASE_URL:  # Use PostgreSQL on Railway
        return psycopg2.connect(DATABASE_URL, sslmode='require')
    else:  # Fallback to SQLite for local use
        conn = sqlite3.connect("stocks.db")
        return conn

# Load Symbols
symbols_df = pd.read_csv("symbols.csv")
symbols = symbols_df["Symbol"].tolist()

st.title("Stock Price & Moving Average App")

# Button to fetch stock data
if st.button("Download Data"):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for symbol in symbols:
        df = yf.download(symbol, period="1y")
        df["Symbol"] = symbol
        
        # Store data in PostgreSQL or SQLite
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO stock_prices (symbol, date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (row["Symbol"], index, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"])
            )

    conn.commit()
    conn.close()
    st.success("Stock data downloaded and stored!")

# Moving Average Inputs
ma1 = st.number_input("Enter first moving average period", min_value=1, value=20)
ma2 = st.number_input("Enter second moving average period", min_value=1, value=50)

# Find Crossovers
if st.button("Find Crossovers"):
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM stock_prices", conn)
    conn.close()

    crossover_stocks = []
    for symbol in symbols:
        stock_df = df[df["symbol"] == symbol]
        stock_df["MA1"] = stock_df["close"].rolling(ma1).mean()
        stock_df["MA2"] = stock_df["close"].rolling(ma2).mean()
        
        if stock_df["MA1"].iloc[-1] > stock_df["MA2"].iloc[-1] and stock_df["MA1"].iloc[-2] <= stock_df["MA2"].iloc[-2]:
            crossover_stocks.append(symbol)

    st.write("Stocks with Moving Average Crossover:", crossover_stocks)
