import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime

BASE_URL = "https://fapi.binance.com"

# ===== Функции работы с Binance =====
def get_top_symbols(limit=50):
    try:
        data = requests.get(f"{BASE_URL}/fapi/v1/ticker/24hr").json()
        df = pd.DataFrame(data)
        df['quoteVolume'] = df['quoteVolume'].astype(float)
        df = df[df['symbol'].str.endswith("USDT")]
        df = df.sort_values('quoteVolume', ascending=False)
        return df['symbol'].tolist()[:limit]
    except:
        return []

def fetch_klines(symbol, interval="5m", limit=200):
    url = f"{BASE_URL}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = requests.get(url).json()
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume","close_time",
        "quote_asset_volume","trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    df = df.astype({
        "open":"float","high":"float","low":"float","close":"float","volume":"float"
    })
    return df

# ===== Индикаторы =====
def EMA(series, period):
    return series.ewm(span=period, adjust=False).mean()

def RSI(series, period=14):
    delta = series.diff()
    gain = np.where(delta>0, delta, 0)
    loss = np.where(delta<0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(period).mean()
    avg_loss = pd.Series(loss).rolling(period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100/(1+rs))
    return rsi

# ===== Анализ одного символа =====
def analyze_symbol(symbol):
    try:
        df5 = fetch_klines(symbol, "5m", 200)
        df15 = fetch_klines(symbol, "15m", 200)
        if len(df5) < 60 or len(df15) < 60:
            return None

        # EMA
        ema9_5 = EMA(df5['close'], 9).iloc[-1]
        ema21_5 = EMA(df5['close'], 21).iloc[-1]
        ema9_15 = EMA(df15['close'], 9).iloc[-1]
        ema21_15 = EMA(df15['close'], 21).iloc[-1]

        # EMA наклон
        ema_slope = EMA(df5['close'], 9).iloc[-1] - EMA(df5['close'], 9).iloc[-4]

        # RSI
        rsi = RSI(df5['close'], 14).iloc[-1]

        # ===== Мягкие фильтры для отбора кандидатов =====
        candidate_long = ema9_15 >= ema21_15 * 0.998 and ema9_5 >= ema21_5 * 0.998
        candidate_short = ema9_15 <= ema21_15 * 1.002 and ema9_5 <= ema21_5 * 1.002

        if not (candidate_long or candidate_short):
            return None  # не кандидат

        # ===== Жёсткая логика сигнала =====
        signal = None
        confidence = 0.0

        if ema_slope > 0 and rsi < 60 and candidate_long:
            signal = "LONG"
            confidence = min(1.0, 0.5 + abs(ema_slope)/0.5)
        elif ema_slope < 0 and rsi > 40 and candidate_short:
            signal = "SHORT"
            confidence = min(1.0, 0.5 + abs(ema_slope)/0.5)

        if signal is None:
            return None

        return {
            "symbol": symbol,
            "signal": signal,
            "confidence": round(confidence, 2),
            "price": df5['close'].iloc[-1],
            "rsi": int(rsi),
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        print(f"Ошибка анализа {symbol}: {e}")
        return None

# ===== Основной анализ =====
def main():
    symbols = get_top_symbols(50)
    results = []
    for s in symbols:
        res = analyze_symbol(s)
        if res:
            results.append(res)

    # сортируем по confidence
    results = sorted(results, key=lambda x: x['confidence'], reverse=True)

    # сохраняем в JSON
    with open("signals.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"{len(results)} сигналов записано в signals.json")

if __name__ == "__main__":
    main()
