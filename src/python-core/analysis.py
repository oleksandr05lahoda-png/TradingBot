def analyze(symbol):
    try:
        # ===== 1. ДАННЫЕ =====
        df5 = fetch_ohlc(symbol, interval="5m", limit=200)
        df15 = fetch_ohlc(symbol, interval="15m", limit=200)

        if len(df5) < 60 or len(df15) < 60:
            return f"{symbol}|NO_TRADE|0.0|-"

        # ===== 2. ИНДИКАТОРЫ =====
        ema9_5 = EMA(df5['close'], 9)
        ema21_5 = EMA(df5['close'], 21)
        ema9_15 = EMA(df15['close'], 9)
        ema21_15 = EMA(df15['close'], 21)

        rsi = RSI(df5['close'], 14)

        # наклон EMA
        ema_slope = ema9_5.iloc[-1] - ema9_5.iloc[-4]

        # ===== 3. ФИЛЬТР АНОМАЛИЙ =====
        atr = ATR(df5, 14)
        last_range = df5['high'].iloc[-1] - df5['low'].iloc[-1]
        if last_range > 2.5 * atr.iloc[-1]:
            return f"{symbol}|NO_TRADE|0.0|ANOMALY"

        # ===== 4. ТРЕНД =====
        trend_long = ema9_15.iloc[-1] > ema21_15.iloc[-1]
        trend_short = ema9_15.iloc[-1] < ema21_15.iloc[-1]

        signal = "NO_TRADE"
        confidence = 0.0

        # ===== 5. LONG =====
        if (
            trend_long and
            ema9_5.iloc[-1] > ema21_5.iloc[-1] and
            ema_slope > 0 and
            rsi.iloc[-2] < 40 and rsi.iloc[-1] > 45
        ):
            signal = "LONG"
            confidence = 0.75

        # ===== 6. SHORT =====
        if (
            trend_short and
            ema9_5.iloc[-1] < ema21_5.iloc[-1] and
            ema_slope < 0 and
            rsi.iloc[-2] > 60 and rsi.iloc[-1] < 55
        ):
            signal = "SHORT"
            confidence = 0.75

        if signal == "NO_TRADE":
            return f"{symbol}|NO_TRADE|0.0|-"

        return f"{symbol}|{signal}|{confidence:.2f}|RSI={int(rsi.iloc[-1])}"

    except Exception:
        return f"{symbol}|ERROR|0.0|-"
