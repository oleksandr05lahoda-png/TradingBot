import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime

BASE_URL = "https://fapi.binance.com"

# ===================== FETCH DATA =====================
def get_top_symbols(limit=50):
    try:
        data = requests.get(f"{BASE_URL}/fapi/v1/ticker/24hr", timeout=10).json()
        df = pd.DataFrame(data)
        df['quoteVolume'] = df['quoteVolume'].astype(float)
        df = df[df['symbol'].str.endswith("USDT")]
        df = df.sort_values('quoteVolume', ascending=False)
        return df['symbol'].tolist()[:limit]
    except:
        return []

def fetch_klines(symbol, interval="5m", limit=200):
    url = f"{BASE_URL}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = requests.get(url, timeout=10).json()
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume","close_time",
        "quote_asset_volume","trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    df = df.astype({"open":"float","high":"float","low":"float","close":"float","volume":"float"})
    return df

# ===================== INDICATORS =====================
def EMA(series, period):
    return series.ewm(span=period, adjust=False).mean()

def RSI(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(period).mean()
    avg_loss = pd.Series(loss).rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def ATR(df, period=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# ===================== PRO FUTURES ENGINE =====================
def analyze_symbol(symbol, forecast_bars=5):
    try:
        df5 = fetch_klines(symbol, "5m", 200)
        df15 = fetch_klines(symbol, "15m", 200)
        if len(df5) < 100 or len(df15) < 100:
            return None

        # ===== EMA & Trend =====
        ema5_5, ema9_5, ema21_5, ema50_5 = [EMA(df5['close'], p) for p in (5,9,21,50)]
        ema9_15, ema21_15 = EMA(df15['close'], 9).iloc[-1], EMA(df15['close'], 21).iloc[-1]
        price = df5['close'].iloc[-1]
        atr = ATR(df5, 14).iloc[-1]
        rsi = RSI(df5['close'], 14).iloc[-1]
        ema_slope = ema9_5.iloc[-1] - ema9_5.iloc[-5]
        trend_15m = "UP" if ema9_15 > ema21_15 else "DOWN"

        # ===== FILTER VOLATILITY =====
        candle_range = df5['high'].iloc[-1] - df5['low'].iloc[-1]
        if candle_range < 0.2 * atr or candle_range > 4.0 * atr:
            return None

        signals = []

        for i in range(forecast_bars):
            sig = None
            conf = 0.50
            score = 0
            reason = ""

            # ===== TREND CONTINUATION =====
            if (trend_15m == "UP" and
                ema5_5.iloc[-1] > ema9_5.iloc[-1] > ema21_5.iloc[-1] > ema50_5.iloc[-1] and
                ema_slope > 0 and 45 < rsi < 78):
                sig = "LONG"
                score += 3
                conf += 0.08
                reason = f"Trend continuation LONG +{i+1}"

            elif (trend_15m == "DOWN" and
                  ema5_5.iloc[-1] < ema9_5.iloc[-1] < ema21_5.iloc[-1] < ema50_5.iloc[-1] and
                  ema_slope < 0 and 22 < rsi < 55):
                sig = "SHORT"
                score += 3
                conf += 0.08
                reason = f"Trend continuation SHORT +{i+1}"

            # ===== PULLBACK ENTRY (ранний вход) =====
            if sig is None:
                if trend_15m == "UP" and ema21_5.iloc[-1] < price < ema9_5.iloc[-1] and rsi > 45:
                    sig = "LONG"
                    score += 2
                    conf += 0.06
                    reason = f"Pullback LONG +{i+1}"
                elif trend_15m == "DOWN" and ema9_5.iloc[-1] < price < ema21_5.iloc[-1] and rsi < 55:
                    sig = "SHORT"
                    score += 2
                    conf += 0.06
                    reason = f"Pullback SHORT +{i+1}"

            # ===== COUNTER-TREND (редко, для ловли разворотов) =====
            if sig is None:
                if rsi < 28 and trend_15m == "UP" and ema_slope > 0:
                    sig = "LONG"
                    score += 2
                    conf += 0.07
                    reason = f"Exhaustion LONG +{i+1}"
                elif rsi > 72 and trend_15m == "DOWN" and ema_slope < 0:
                    sig = "SHORT"
                    score += 2
                    conf += 0.07
                    reason = f"Exhaustion SHORT +{i+1}"

            if sig and score >= 2:
                signals.append({
                    "symbol": symbol,
                    "signal": sig,
                    "confidence": round(min(conf, 0.88),2),
                    "price": round(price,6),
                    "rsi": int(rsi),
                    "trend_15m": trend_15m,
                    "reason": reason,
                    "timestamp": datetime.utcnow().isoformat(),
                    "forecast_bar": i+1
                })

        if not signals:
            return None
        return signals

    except Exception as e:
        print(f"Ошибка анализа {symbol}: {e}")
        return None

# ===================== MAIN =====================
def main():
    symbols = get_top_symbols(50)
    all_signals = []

    for s in symbols:
        res = analyze_symbol(s)
        if res:
            all_signals.extend(res)

    all_signals = sorted(all_signals, key=lambda x: x['confidence'], reverse=True)

    with open("signals.json", "w") as f:
        json.dump(all_signals, f, indent=2)

    print(f"{len(all_signals)} PRO signals saved")

if __name__ == "__main__":
    main()
