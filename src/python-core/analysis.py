#!/usr/bin/env python3
import os
import requests
import numpy as np
import pandas as pd
import mplfinance as mpf
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Параметры через ENV
COINS_ENV = os.getenv("COINS")  # примеры: BTCUSDT,ETHUSDT,BNBUSDT
if COINS_ENV:
    COINS = [c.strip().upper() for c in COINS_ENV.split(",")]
else:
    COINS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]

CANDLES = int(os.getenv("CANDLES", "100"))
OUT_DIR = os.getenv("OUT_DIR", "charts")
os.makedirs(OUT_DIR, exist_ok=True)

BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

def fetch_prices_binance(symbol, interval="1m", limit=200):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(BINANCE_KLINES, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    # формируем DataFrame с OHLC для свечного графика
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume","close_time",
        "quote_asset_volume","trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    df = df[["open","high","low","close","volume"]].astype(float)
    df.index = pd.to_datetime([x[0] for x in data], unit='ms')
    return df

# индикаторы
def SMA(series, n):
    return series.rolling(n).mean()

def RSI(series, n=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/n, adjust=False).mean()
    ma_down = down.ewm(alpha=1/n, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def features_from_prices(prices):
    s = pd.Series(prices)
    df = pd.DataFrame()
    df['close'] = s
    df['ret1'] = s.pct_change(1).fillna(0)
    df['ret3'] = s.pct_change(3).fillna(0)
    df['sma5'] = SMA(s, 5).fillna(method='bfill')
    df['sma20'] = SMA(s, 20).fillna(method='bfill')
    df['sma_ratio'] = (df['sma5'] / (df['sma20'] + 1e-9)).fillna(1.0)
    df['rsi14'] = RSI(s, 14).fillna(50)
    df['mom3'] = s.diff(3).fillna(0)
    df['vol5'] = s.pct_change().rolling(5).std().fillna(0)
    return df

def prepare_dataset(prices, lookback=CANDLES):
    df = features_from_prices(prices)
    df = df.iloc[-(lookback+5):].reset_index(drop=True)
    df['target'] = (df['close'].shift(-1) > df['close']).astype(int)
    df = df.dropna().reset_index(drop=True)
    X = df[['ret1','ret3','sma_ratio','rsi14','mom3','vol5']].values
    y = df['target'].values
    return X, y, df

def train_models(X, y):
    if len(y) < 10:
        return None, None
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=True, random_state=42)
    lr = LogisticRegression(max_iter=1000)
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    try:
        lr.fit(X_train, y_train)
        rf.fit(X_train, y_train)
    except:
        return None, None
    return lr, rf

def predict_ensemble(models, X_last):
    lr, rf = models
    probs = []
    if lr: probs.append(lr.predict_proba(X_last.reshape(1, -1))[0][1])
    if rf: probs.append(rf.predict_proba(X_last.reshape(1, -1))[0][1])
    return float(np.mean(probs)) if probs else 0.5

def analyze_coin(symbol):
    try:
        df_ohlc = fetch_prices_binance(symbol, limit=max(200, CANDLES+20))
        closes = df_ohlc['close'].values
        if len(closes) < CANDLES + 2:
            return f"{symbol}|ERROR|0.0|-"
        X, y, df_feat = prepare_dataset(closes, lookback=CANDLES)
        if X.shape[0] < 10:
            return f"{symbol}|NEUTRAL|0.0|-"
        models = train_models(X, y)
        X_last = X[-1]
        prob = predict_ensemble(models, X_last)
        signal = "LONG" if prob > 0.52 else ("SHORT" if prob < 0.48 else "NEUTRAL")
        confidence = abs(prob - 0.5) * 2
        chart_path = os.path.join(OUT_DIR, f"{symbol}_chart.png")
        # Рисуем свечной график через mplfinance
        mpf.plot(df_ohlc[-CANDLES:], type='candle', style='charles',
                 title=f"{symbol} {signal} prob={prob:.3f}",
                 savefig=dict(fname=chart_path, dpi=100))
        return f"{symbol}|{signal}|{confidence:.3f}|{chart_path}"
    except Exception as e:
        return f"{symbol}|ERROR|0.0|-"

if __name__ == "__main__":
    for coin in COINS:
        out = analyze_coin(coin)
        print(out)
