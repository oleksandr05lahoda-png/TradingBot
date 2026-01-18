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
    df = df.astype({"open":"float","high":"float","low":"float","close":"float","volume":"float"})
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

def ATR(df, period=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return atr

# ===== Анализ символа и прогноз =====
def analyze_symbol(symbol, forecast_bars=5):
    try:
        df5 = fetch_klines(symbol, "5m", 200)
        df15 = fetch_klines(symbol, "15m", 200)
        if len(df5) < 60 or len(df15) < 60:
            return None

        # ===== EMA =====
        ema9_5 = EMA(df5['close'], 9)
        ema21_5 = EMA(df5['close'], 21)
        ema9_15 = EMA(df15['close'], 9).iloc[-1]
        ema21_15 = EMA(df15['close'], 21).iloc[-1]

        ema_slope = ema9_5.iloc[-1] - ema9_5.iloc[-5]

        # RSI
        rsi = RSI(df5['close'], 14).iloc[-1]

        # ATR
        atr = ATR(df5, 14).iloc[-1]
        last_3_range = (df5['high'].iloc[-3:] - df5['low'].iloc[-3:]).mean()
        if last_3_range > 1.5 * atr:
            return None

        # ===== Фильтры: флет, сильный импульс, неопределенность RSI =====
        if 40 < rsi < 60:  # рынок не решил
            return None
        current_range = df5['high'].iloc[-1] - df5['low'].iloc[-1]
        if current_range > 1.5 * atr:
            return None
        trend_15m = "UP" if ema9_15 > ema21_15 else "DOWN"

        # ===== Контртрендовые входы =====
        signals = []
            sig = None
            conf = 0.55

            # Лонг: истощение вниз, готов разворот
            if rsi < 35 and ema9_5.iloc[-1] < ema21_5.iloc[-1] and ema_slope > 0 and trend_15m == "UP":
                sig = "LONG"
                conf += 0.05
            # Шорт: истощение вверх, готов разворот
            elif rsi > 65 and ema9_5.iloc[-1] > ema21_5.iloc[-1] and ema_slope < 0 and trend_15m == "DOWN":
                sig = "SHORT"
                conf += 0.05

            if sig:
                signals.append({
                    "symbol": symbol,
                    "signal": sig,
                    "confidence": round(min(conf, 0.65), 2),
                    "price": df5['close'].iloc[-1],
                    "rsi": int(rsi),
                    "timestamp": datetime.utcnow().isoformat(),
                    "forecast_bar": i+1
                })

        if not signals:
            return None
        return signals

    except Exception as e:
        print(f"Ошибка анализа {symbol}: {e}")
        return None

# ===== Основной анализ =====
def main():
    symbols = get_top_symbols(50)
    all_signals = []
    for s in symbols:
        res = analyze_symbol(s)
        if res:
            all_signals.extend(res)

    # сортируем по confidence
    all_signals = sorted(all_signals, key=lambda x: x['confidence'], reverse=True)

    # сохраняем в JSON
    with open("signals.json", "w") as f:
        json.dump(all_signals, f, indent=2)

    print(f"{len(all_signals)} сигналов записано в signals.json")

if __name__ == "__main__":
    main()
