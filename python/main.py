import pandas as pd
import mysql.connector as mysql
import requests

# MySQL connection setup (update credentials as needed)\
conn = mysql.connect(
    host='127.0.0.1',
    user='root',
    password='heisenberg',
    database='stocks',
    charset='utf8mb4',
    collation='utf8mb4_unicode_ci'
)

cursor = conn.cursor()

#specify the tickers that you are interested in
tickers = ["MSFT", "IBM"]

def fetch_d(ticker, interval='5min'):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&apikey=OKBWHTFPHERJ7JQV'
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
    
    if True:
        a = input("Enter 'a' to fetch and store data, 'b' to query data, 'c' to exit: ")
        if a == 'a':
            for ticker in tickers:
                condb(ticker)
                latest_timestamp = latest_stamp(ticker)
                data = fetch_d(ticker)
                store2db(data, latest_timestamp)
        if a == 'b':
            b = input("Enter ticker to query: ")
            cursor.execute(f"SELECT * FROM stocks_{b};")
            results = cursor.fetchall()
            for result in results:
                print(result)
        if a == 'c':
            exit()

# Close connection
conn.close()
