import mysql.connector
from mysql.connector import Error
import yfinance as yf
import json
from datetime import datetime, timedelta
import pandas as pd

def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='stock_data',
            user='root',
            password='taikisawa43',
            charset='utf8mb4',
            collation='utf8mb4_general_ci',
            use_unicode=True
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    return None

def create_stock_price_table(connection):
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_price (
        stock_code VARCHAR(10) PRIMARY KEY,
        stock_price_data JSON,
        FOREIGN KEY (stock_code) REFERENCES stock(code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    connection.commit()
    print("stock_price table created successfully")

def get_stock_data(stock_code):
    try:

        ticker=stock_code+'.T'
        print(ticker)

        # Yahoo Finance APIを使用してデータを取得
        ticker_data = yf.Ticker(ticker)
        hist = ticker_data.history(period="max",interval="1wk")
        
        if hist.empty:
            print(f"No historical data found for ticker {ticker}")
            return None
        
        # 配当金データを取得
        dividends = ticker_data.dividends.reset_index()
        if dividends.empty:
            print(f"No dividend data found for ticker {ticker}. Setting dividend to 0.")
            dividends = pd.DataFrame({'date': hist.index, 'dividend': [0] * len(hist)})  # histの日付を使用
        dividends = dividends.rename(columns={'Date': 'date', 'Dividends': 'dividend'})
        
        # 年ごとに配当金を合計
        dividends['year'] = dividends['date'].dt.year
        annual_dividends = dividends.groupby('year')['dividend'].sum().reset_index()
        annual_dividends['date'] = annual_dividends['year'].astype(str)  # 年を文字列に変換

        # データを整形
        result = hist[['Close']].reset_index()
        result = result.rename(columns={
            'Date': 'date',
            'Close': 'price'
        })

        # 日付をISO形式の文字列に変換
        result['date'] = result['date'].dt.strftime('%Y-%m-%d')

        # 年ごとの配当金を株価データの日付に基づいてマージ
        result['year'] = pd.to_datetime(result['date']).dt.year
        result = result.merge(annual_dividends[['year', 'dividend']], on='year', how='left')

        # 最終的なJSONデータを整形
        result = result[['date', 'price', 'dividend']].fillna(0)  # NaNを0に置き換え

        return json.dumps(result.to_dict(orient='records'))

    except Exception as e:
        print(f"Error occurred while fetching data for {stock_code}: {str(e)}")
        return None

def insert_stock_price_data(connection):
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_price (
        stock_code VARCHAR(10) PRIMARY KEY,
        stock_price_data JSON
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    
    # Get all stock codes from the stock table
    cursor.execute("SELECT code FROM stock")
    stock_codes = cursor.fetchall()
    loopCount=0
    for (stock_code,) in stock_codes:
        loopCount = loopCount+1
        stock_price_data = get_stock_data(stock_code)
        if stock_price_data:
            cursor.execute("""
            INSERT INTO stock_price (stock_code, stock_price_data)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE stock_price_data = VALUES(stock_price_data)
            """, (stock_code, stock_price_data))
        if loopCount==50:
            break

    
    connection.commit()
    print("Stock price data inserted successfully")

def main():
    connection = create_connection()
    if connection is None:
        return
    
    create_stock_price_table(connection)
    insert_stock_price_data(connection)
    
    connection.close()
    print("Database connection closed")

if __name__ == "__main__":
    main()