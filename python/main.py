import pandas as pd
import mysql.connector as mysql
import requests
import sys
import os
import json

arg = sys.argv
with open('jsonsyn.json', 'r') as f:
    jsonsyn = json.load(f)

# Enter your API key
apikey = 'OKBWHTFPHERJ7JQV'

# MySQL connection setup (update credentials as needed)
conn = mysql.connect(
    host='localhost',
    user='root',
    password='password',
    database='stocks',
    charset='utf8mb4',
    collation='utf8mb4_unicode_ci'
)

cursor = conn.cursor()

# Specify the tickers that you are interested in
tickers = jsonsyn.get('tickers')

def fetch_d(ticker, interval='5min'):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&apikey={apikey}'
    response = requests.get(url)
    data = response.json()

    time_series = data.get(f'Time Series ({interval})', {})
    if not time_series:
        print(f"No data found for {ticker}")
        return None

    df = pd.DataFrame(time_series).T
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df['timestamp'] = df.index
    df['ticker'] = ticker
    df = df.astype({'close': 'float', 'volume': 'float'})  # Ensure correct data types
    return df

def condb(ticker):
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS stocks_{ticker} (
        ticker VARCHAR(5),
        timestamp VARCHAR(45) PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INT        
    )
    ''')
    conn.commit()

def latest_stamp(ticker):
    query = f"SELECT MAX(timestamp) FROM stocks_{ticker}"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result

def store2db(data, lat_stamp):
    if data is not None:
        if lat_stamp:
            data = data[data['timestamp'] > lat_stamp]
        if not data.empty:
            # Insert data row by row into MySQL
            for _, row in data.iterrows():
                cursor.execute(f'''
                INSERT INTO stocks_{row['ticker']} (ticker, timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open),
                    high=VALUES(high),
                    low=VALUES(low),
                    close=VALUES(close),
                    volume=VALUES(volume)
                ''', (row['ticker'], row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume']))
            conn.commit()
            print(f"Data for {data['ticker'].iloc[0]} stored successfully")
        else:
            print("No new data to store")
    else:
        print("No data to store")

# SQL-based VWMA calculation
def sql_vwma(ticker, period=20):
    query = f'''
        SELECT 
            timestamp,
            SUM(close * volume) / SUM(volume) AS VWMA
        FROM (
            SELECT close, volume, timestamp 
            FROM stocks_{ticker}
            ORDER BY timestamp DESC
            LIMIT {period}
        ) AS subquery;
    '''
    cursor.execute(query)
    result = cursor.fetchone()
    if result and result[1] is not None:
        print(f"VWMA for {ticker} over the last {period} periods: {result[1]} at timestamp {result[0]}")
    else:
        print(f"No data available for VWMA calculation for {ticker}")

# SQL-based OBV calculation
# SQL-based OBV calculation
def sql_obv(ticker):
    query = f'''
        WITH obv_cte AS (
            SELECT timestamp, close, volume,
            LAG(close, 1) OVER (ORDER BY timestamp) AS prev_close
            FROM stocks_{ticker}
        )
        SELECT timestamp, 
        SUM(
            CASE
                WHEN close > prev_close THEN volume
                WHEN close < prev_close THEN -volume
                ELSE 0
            END
        ) AS OBV
        FROM obv_cte
        GROUP BY timestamp
        ORDER BY timestamp DESC
        LIMIT 1;
    '''
    cursor.execute(query)
    result = cursor.fetchone()
    if result and result[1] is not None:
        print(f"OBV for {ticker}: {result[1]} at timestamp {result[0]}")
    else:
        print(f"No data available for OBV calculation for {ticker}")

# SQL-based ADX calculation
# SQL-based ADX calculation
def sql_adx(ticker, period=14):
    query = f'''
        WITH dm_tr AS (
            -- Step 1: Calculate directional movement and true range
            SELECT timestamp, 
            GREATEST(high - LAG(high, 1) OVER (ORDER BY timestamp), 0) AS plus_dm,
            GREATEST(LAG(low, 1) OVER (ORDER BY timestamp) - low, 0) AS minus_dm,
            GREATEST(high - low, 
                     ABS(high - LAG(close, 1) OVER (ORDER BY timestamp)), 
                     ABS(low - LAG(close, 1) OVER (ORDER BY timestamp))) AS true_range
            FROM stocks_{ticker}
        ),
        di AS (
            -- Step 2: Calculate the Directional Indicators (+DI and -DI)
            SELECT timestamp,
            100 * SUM(plus_dm) OVER (ORDER BY timestamp ROWS {period - 1} PRECEDING) / SUM(true_range) OVER (ORDER BY timestamp ROWS {period - 1} PRECEDING) AS plus_di,
            100 * SUM(minus_dm) OVER (ORDER BY timestamp ROWS {period - 1} PRECEDING) / SUM(true_range) OVER (ORDER BY timestamp ROWS {period - 1} PRECEDING) AS minus_di
            FROM dm_tr
        ),
        adx_cte AS (
            -- Step 3: Calculate the ADX
            SELECT timestamp, 
            100 * AVG(ABS(plus_di - minus_di) / (plus_di + minus_di)) OVER (ORDER BY timestamp ROWS {period - 1} PRECEDING) AS adx
            FROM di
        )
        -- Select the most recent ADX value
        SELECT timestamp, adx
        FROM adx_cte
        ORDER BY timestamp DESC
        LIMIT 1;
    '''
    cursor.execute(query)
    result = cursor.fetchone()
    if result and result[1] is not None:
        print(f"ADX for {ticker} over the last {period} periods: {result[1]} at timestamp {result[0]}")
    else:
        print(f"No data available for ADX calculation for {ticker}")

def main():
    
    if arg[1] == "refresh":
        for ticker in tickers:
            condb(ticker)
            latest_timestamp = latest_stamp(ticker)
            data = fetch_d(ticker)
            store2db(data, latest_timestamp)
    
    elif arg[1] == "show":
        try:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            if f"stocks_{arg[2].upper()}" not in [table[0] for table in tables]:
                print("ticker doesn't exist")
            else:
                cursor.execute(f"SELECT * FROM stocks_{arg[2].upper()};")
                results = cursor.fetchall()
                for result in results:
                    print(result)
        except IndexError:
            print("please pass a ticker arguement")
    
    elif arg[1] == "add":
        try:
            tickers.append(arg[2].upper())
            jsonsyn['tickers'] = tickers
            with open('jsonsyn.json', 'w') as f:
                json.dump(jsonsyn, f, indent=4)
            print(f"Ticker {arg[2].upper()} added.")
        except IndexError:
            print("please provide a ticker arguement")
    
    elif arg[1] == "VWMA":
        try:
            ticker = arg[2].upper()
            period = int(arg[3]) if len(arg) > 3 else 20
            sql_vwma(ticker, period)
        except IndexError:
            print("please provide a ticker argument for VWMA calculation")
    
    elif arg[1] == "OBV":
        try:
            ticker = arg[2].upper()
            sql_obv(ticker)
        except IndexError:
            print("please provide a ticker argument for OBV calculation")
    
    elif arg[1] == "ADX":
        try:
            ticker = arg[2].upper()
            period = int(arg[3]) if len(arg) > 3 else 14
            sql_adx(ticker, period)
        except IndexError:
            print("please provide a ticker argument for ADX calculation")

if __name__ == "__main__":
    main()

# Close connection
conn.close()
