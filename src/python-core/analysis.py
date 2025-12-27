#!/usr/bin/env python3
import os, sys, json, math, requests
import numpy as np
import pandas as pd
import mplfinance as mpf

# CONFIG from ENV or defaults
COINS_ENV = os.getenv("COINS")
if COINS_ENV:
    COINS = [c.strip().upper() for c in COINS_ENV.split(",")]
else:
    COINS = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","ADAUSDT"]

CANDLES = int(os.getenv("CANDLES","100"))   # number of candles used for ML + chart
INTERVAL = os.getenv("INTERVAL","5m")       # 5m timeframe
OUT_DIR = os.getenv("OUT_DIR","charts")
os.makedirs(OUT_DIR, exist_ok=True)

BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

# fetch OHLC
def fetch_ohlc(symbol, interval=INTERVAL, limit=200):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(BINANCE_KLINES, params=params, timeout=10)
    r.raise_for_status()
    raw = r.json()
    df = pd.DataFrame(raw, columns=[
        "open_time","open","high","low","close","volume","close_time",
        "quote_asset_volume","trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    df = df[["open","high","low","close","volume","quote_asset_volume"]].astype(float)
    df.index = pd.to_datetime([row[0] for row in raw], unit='ms')
    return df

# indicators
def SMA(series, n): return series.rolling(n).mean()
def EMA(series, n): return series.ewm(span=n, adjust=False).mean()
def RSI(series, n=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/n, adjust=False).mean()
    ma_down = down.ewm(alpha=1/n, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-9)
    return 100 - (100 / (1 + rs))
def MACD(series, fast=12, slow=26, signal=9):
    efast = EMA(series, fast)
    eslow = EMA(series, slow)
    macd_line = efast - eslow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def ATR(df, n=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(n).mean()
    return atr.fillna(0)

def add_features(df):
    s = df['close']
    df_feat = pd.DataFrame(index=df.index)
    df_feat['close'] = s
    df_feat['ret1'] = s.pct_change(1).fillna(0)
    df_feat['ret3'] = s.pct_change(3).fillna(0)
    df_feat['ema9'] = EMA(s,9).fillna(method='bfill')
    df_feat['ema21'] = EMA(s,21).fillna(method='bfill')
    df_feat['ema_diff'] = (df_feat['ema9'] - df_feat['ema21']) / (df_feat['ema21'] + 1e-9)
    m, sig, h = MACD(s)
    df_feat['macd'] = m.fillna(0)
    df_feat['macd_sig'] = sig.fillna(0)
    df_feat['macd_hist'] = h.fillna(0)
    df_feat['rsi14'] = RSI(s,14).fillna(50)
    df_feat['mom3'] = s.diff(3).fillna(0)
    df_feat['vol5'] = s.pct_change().rolling(5).std().fillna(0)
    # normalized volume (quote_asset_volume)
    if 'quote_asset_volume' in df.columns:
        df_feat['qvol'] = df['quote_asset_volume'].fillna(0)
    else:
        df_feat['qvol'] = df['volume'].fillna(0)
    return df_feat

def prepare_dataset(df_ohlc, lookback=CANDLES):
    df_feat = add_features(df_ohlc)
    df_feat = df_feat.iloc[-(lookback+5):].reset_index(drop=True)
    df_feat = df_feat.dropna().reset_index(drop=True)
    X = df_feat[['ret1','ret3','ema_diff','macd_hist','rsi14','mom3','vol5']].values
    y = df_feat['target'].values
    return X, y, df_feat

def draw_candles(df_ohlc, symbol, signal):
    chart_path = os.path.join(OUT_DIR, f"{symbol}_chart.png")
    df_for_plot = df_ohlc[-CANDLES:].copy()
    df_for_plot.index = pd.date_range(end=pd.Timestamp.now(), periods=len(df_for_plot), freq=INTERVAL)
    mpf.plot(df_for_plot, type='candle', style='charles', title=f"{symbol} {signal}", savefig=dict(fname=chart_path,dpi=100))
    return chart_path

def analyze(symbol):
    try:
        # 1️⃣ Загружаем данные
        df_ohlc = fetch_ohlc(symbol, interval=INTERVAL, limit=max(200, CANDLES+20))
        closes = df_ohlc['close'].values
        if len(closes) < CANDLES+2:
            return f"{symbol}|ERROR|0.0|-"

        # 2️⃣ Подготавливаем фичи
        X, y, df_feat = prepare_dataset(df_ohlc, lookback=CANDLES)
        if X.shape[0] < 10:
            return f"{symbol}|NEUTRAL|0.0|-"

        # 3️⃣ Фильтр аномалий через ATR
        atr = ATR(df_ohlc, 14)
        last_range = df_ohlc['high'].iloc[-1] - df_ohlc['low'].iloc[-1]
        if last_range > 2.5 * atr.iloc[-1]:
            return f"{symbol}|NO_TRADE|0.0|ANOMALY"

        # 4️⃣ Новая логика сигналов без моделей
        ema9 = df_feat['ema9'].iloc[-1]
        ema21 = df_feat['ema21'].iloc[-1]
        rsi = df_feat['rsi14'].iloc[-1]

        signal = "NO_TRADE"
        if ema9 > ema21 and rsi < 65:
            signal = "WATCH_LONG"
        elif ema9 < ema21 and rsi > 35:
            signal = "WATCH_SHORT"

        # 5️⃣ Конфиденс (можно оставить простым для сигналов без ML)
        confidence = 0.7 if signal.startswith("WATCH") else 0.0

        # 6️⃣ Рисуем график
        chart = draw_candles(df_ohlc, symbol, signal)

return f"{symbol}|{signal}|{confidence:.2f}|RSI={int(rsi)}"

    except Exception as e:
        return f"{symbol}|ERROR|0.0|-"

# If script called with arguments: single coin or JSON list
if __name__ == "__main__":
    # optional: pass coin as arg
    if len(sys.argv) > 1:
        coins = [sys.argv[1]]
    else:
        coins = COINS
    for coin in coins:
        print(analyze(coin))
