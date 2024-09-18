import pandas as pd
import mysql.connector as mysql
import requests
import sys
import os
import json

arg = sys.argv
with open('jsonsyn.json', 'r') as f:
    jsonsyn = json.load(f)

#Enter your API key
apikey = 'OKBWHTFPHERJ7JQV'

# MySQL connection setup (update credentials as needed)\
conn = mysql.connect(
    host='127.0.0.1',
    user='user',
    password='password',
    database='stocks',
    charset='utf8mb4',
    collation='utf8mb4_unicode_ci'
)

cursor = conn.cursor()

#specify the tickers that you are interested in
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

def main():
    
    if arg[1] == "refresh":
        for ticker in tickers:
            condb(ticker)
            latest_timestamp = latest_stamp(ticker)
            data = fetch_d(ticker)
            store2db(data, latest_timestamp)
    if arg[1] == "show":
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
    if arg[1] == "add":
        try:
            tickers.append(arg[2].upper())
            jsonsyn['tickers'] = tickers
            with open('jsonsyn.json', 'w') as f:
                json.dump(jsonsyn, f, indent=4)
            print(f"Ticker {arg[2].upper()} added.")
        except IndexError:
            print("please provide a ticker arguement")

if __name__ == "__main__":
    main()

# Close connection
conn.close()
