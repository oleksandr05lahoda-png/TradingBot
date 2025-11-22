#!/usr/bin/env python3
import os
import math
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Параметры через ENV
COINS_ENV = os.getenv("COINS")  # примеры: BTCUSDT,ETHUSDT,BNBUSDT
if COINS_ENV:
    COINS = [c.strip().upper() for c in COINS_ENV.split(",")]
else:
    COINS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]  # дефолт список топ монет

CANDLES = int(os.getenv("CANDLES", "100"))   # сколько свечей брать (default 100)
OUT_DIR = os.getenv("OUT_DIR", "charts")
os.makedirs(OUT_DIR, exist_ok=True)

BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

def fetch_prices_binance(symbol, interval="1m", limit=200):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(BINANCE_KLINES, params=params, timeout=10)
    if r.status_code != 200:
        raise Exception(f"Binance API error {r.status_code}")
    data = r.json()
    closes = [float(x[4]) for x in data]
    return closes

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
    # формируем X, y. y = 1 если следующая свеча (close_{t+1}) выше текущей
    df = features_from_prices(prices)
    # оставляем только последние lookback+1 точек, чтобы иметь метки
    df = df.iloc[-(lookback+5):].reset_index(drop=True)
    # метки
    df['target'] = (df['close'].shift(-1) > df['close']).astype(int)
    df = df.dropna().reset_index(drop=True)
    # features and labels
    X = df[['ret1','ret3','sma_ratio','rsi14','mom3','vol5']].values
    y = df['target'].values
    return X, y, df

def train_models(X, y):
    # небольшой ансамбль: логрег + случайный лес
    if len(y) < 10:
        return None, None
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=True, random_state=42)
    lr = LogisticRegression(max_iter=1000)
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    try:
        lr.fit(X_train, y_train)
        rf.fit(X_train, y_train)
    except Exception as e:
        # если обучение не сходится, вернём None
        return None, None
    return lr, rf

def predict_ensemble(models, X_last):
    lr, rf = models
    probs = []
    if lr:
        p1 = lr.predict_proba(X_last.reshape(1, -1))[0][1]
        probs.append(p1)
    if rf:
        p2 = rf.predict_proba(X_last.reshape(1, -1))[0][1]
        probs.append(p2)
    if not probs:
        return 0.5
    return float(np.mean(probs))

def analyze_coin(symbol):
    try:
        closes = fetch_prices_binance(symbol, limit=max(200, CANDLES + 20))
        if len(closes) < CANDLES + 2:
            return f"{symbol}|ERROR|0.0|-"
        # берём последние CANDLES+10 для устойчивости
        closes = closes[-(CANDLES + 10):]
        X, y, df = prepare_dataset(closes, lookback=CANDLES)
        if X.shape[0] < 10:
            return f"{symbol}|NEUTRAL|0.0|-"
        # обучаем модели
        models = train_models(X, y)
        # используем последнюю строку для предсказания
        X_last = X[-1]
        prob = predict_ensemble(models, X_last)
        # переводим вероятность в сигнал и confidence
        signal = "LONG" if prob > 0.52 else ("SHORT" if prob < 0.48 else "NEUTRAL")
        confidence = abs(prob - 0.5) * 2  # 0..1 scale, 0 = 50/50, 1 = certainty
        # рисуем график последних CANDLES
        chart_path = os.path.join(OUT_DIR, f"{symbol}_chart.png")
        plt.figure(figsize=(8,4))
        plt.plot(closes[-CANDLES:], marker='o', linewidth=1)
        plt.title(f"{symbol} {signal} prob={prob:.3f}")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(chart_path)
        plt.close()
        return f"{symbol}|{signal}|{confidence:.3f}|{chart_path}"
    except Exception as e:
        return f"{symbol}|ERROR|0.0|-"

if __name__ == "__main__":
    for coin in COINS:
        out = analyze_coin(coin)
        print(out)
