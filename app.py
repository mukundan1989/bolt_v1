import streamlit as st
import pandas as pd
import yfinance as yf
import psycopg2
import os

# Get PostgreSQL connection URL from Railway's environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL)

def create_table():
    """Reads create_table.sql and executes it to create the table if not exists."""
    conn = get_db_connection()
    cursor = conn.cursor()

    with open("create_table.sql", "r") as sql_file:
        sql_script = sql_file.read()

    cursor.execute(sql_script)
    conn.commit()
    cursor.close()
    conn.close()

# Call function to create table on startup
create_table()

def fetch_stock_data(symbol):
    """Fetch historical stock prices for a given symbol."""
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(period="1y")  # Get 1 year of data
        df.reset_index(inplace=True)
        df["Symbol"] = symbol  # Add a symbol column
        return df
    except Exception as e:
        st.error(f"Error fetching data for {symbol}: {e}")
        return None

def save_to_database(df):
    """Save stock data to PostgreSQL database."""
    if df is None or df.empty:
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO NOTHING;
        """, (row["Symbol"], row["Date"], row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]))

    conn.commit()
    cursor.close()
    conn.close()

def get_moving_average_crossovers(short_window, long_window):
    """Find stocks where short-term MA crosses above long-term MA."""
    conn = get_db_connection()
    query = f"SELECT DISTINCT symbol FROM stock_prices;"
    df_symbols = pd.read_sql(query, conn)
    conn.close()

    crossover_stocks = []

    for symbol in df_symbols["symbol"]:
        conn = get_db_connection()
        query = f"SELECT date, close FROM stock_prices WHERE symbol = '{symbol}' ORDER BY date ASC;"
        df = pd.read_sql(query, conn)
        conn.close()

        if df.shape[0] < long_window:
            continue  # Skip if not enough data

        df["Short_MA"] = df["close"].rolling(window=short_window).mean()
        df["Long_MA"] = df["close"].rolling(window=long_window).mean()
        df.dropna(inplace=True)

        # Detect crossover
        if df.iloc[-2]["Short_MA"] < df.iloc[-2]["Long_MA"] and df.iloc[-1]["Short_MA"] > df.iloc[-1]["Long_MA"]:
            crossover_stocks.append(symbol)

    return crossover_stocks

# Streamlit UI
st.title("Stock Moving Average Crossover Finder")

# Fetch & Store Data
if st.button("Download Data"):
    symbols_df = pd.read_csv("symbols.csv")  # Load symbols from CSV
    for symbol in symbols_df["Symbol"]:
        stock_data = fetch_stock_data(symbol)
        save_to_database(stock_data)
    st.success("Stock data downloaded and stored in database!")

# Moving Average Crossover Inputs
st.subheader("Find Moving Average Crossovers")
short_window = st.number_input("Enter Short Moving Average Period", min_value=1, value=20)
long_window = st.number_input("Enter Long Moving Average Period", min_value=1, value=50)

if st.button("Find Crossovers"):
    results = get_moving_average_crossovers(short_window, long_window)
    if results:
        st.write("Stocks with Moving Average Crossover:")
        st.write(results)
    else:
        st.write("No stocks found with the selected moving average crossover.")
