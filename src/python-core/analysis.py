import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import deque

BASE_URL = "https://fapi.binance.com"

class Elite5MinAnalyzer:
    """Professional 5-minute futures analyzer for early entries, reversals, and trend detection"""

    def __init__(self, min_confidence=0.45, max_confidence=0.95, cooldown_minutes=5):
        self.min_confidence = min_confidence
        self.max_confidence = max_confidence
        self.cooldown_minutes = cooldown_minutes
        self.last_signal_time = {}
        self.history = {}

    # ===================== FETCH =====================
    def fetch_klines(self, symbol, interval="5m", limit=500):
        url = f"{BASE_URL}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
        data = requests.get(url, timeout=10).json()
        df = pd.DataFrame(data, columns=[
            "open_time","open","high","low","close","volume","close_time",
            "qav","trades","tb","tq","ignore"
        ])
        df = df.astype({"open":"float","high":"float","low":"float",
                        "close":"float","volume":"float"})
        return df

    # ===================== INDICATORS =====================
    @staticmethod
    def EMA(series, period):
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def RSI(series, period=14):
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = -delta.clip(upper=0).rolling(period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ATR(df, period=14):
        tr = pd.concat([
            df.high - df.low,
            (df.high - df.close.shift()).abs(),
            (df.low - df.close.shift()).abs()
        ], axis=1).max(axis=1)
        return tr.rolling(period).mean()

    # ===================== CONTEXT =====================
    def build_context(self, df5, df15, df1h, df4h):
        context = {}
        price = df5.close.iloc[-1]
        atr = self.ATR(df5).iloc[-1]

        # EMA slopes for micro trend detection
        ema9_slope = self.EMA(df5.close, 9).iloc[-1] - self.EMA(df5.close, 9).iloc[-5]
        ema21_slope = self.EMA(df5.close, 21).iloc[-1] - self.EMA(df5.close, 21).iloc[-5]

        context.update({
            "price": price,
            "atr": atr,
            "ema9_slope": ema9_slope,
            "ema21_slope": ema21_slope,
            "rsi": self.RSI(df5.close).iloc[-1],
            "compressed": (df5.high.iloc[-1]-df5.low.iloc[-1]) < (df5.high-df5.low).rolling(20).mean().iloc[-1]*0.7,
            "vol_climax": df5.volume.iloc[-1] > df5.volume.rolling(30).mean().iloc[-1]*1.5
        })

        # Higher time frame trends
        context["htf_15m"] = "UP" if self.EMA(df15.close, 9).iloc[-1] > self.EMA(df15.close, 21).iloc[-1] else "DOWN"
        context["htf_1h"] = "UP" if self.EMA(df1h.close, 9).iloc[-1] > self.EMA(df1h.close, 21).iloc[-1] else "DOWN"
        context["htf_4h"] = "UP" if self.EMA(df4h.close, 9).iloc[-1] > self.EMA(df4h.close, 21).iloc[-1] else "DOWN"

        # Liquidity sweeps
        context["sweep_high"] = df5.high.iloc[-1] > df5.high.iloc[-20:-1].max() and (df5.high.iloc[-1]-df5.close.iloc[-1]) > atr*0.2
        context["sweep_low"] = df5.low.iloc[-1] < df5.low.iloc[-20:-1].min() and (df5.close.iloc[-1]-df5.low.iloc[-1]) > atr*0.2

        return context

    # ===================== SIGNAL GENERATION =====================
    def generate_signals(self, symbol):
        df5 = self.fetch_klines(symbol, "5m", 500)
        df15 = self.fetch_klines(symbol, "15m", 200)
        df1h = self.fetch_klines(symbol, "1h", 200)
        df4h = self.fetch_klines(symbol, "4h", 100)

        if len(df5) < 120: return []

        ctx = self.build_context(df5, df15, df1h, df4h)
        price = ctx["price"]
        signals = []

        # ===== REVERSAL LONG =====
        if ctx["sweep_low"] and ctx["rsi"] < 35 and ctx["vol_climax"]:
            conf = 0.58
            conf += 0.05 if ctx["compressed"] else 0
            conf += 0.04 if ctx["htf_15m"] == "UP" else 0
            signals.append(self._build_signal(symbol, "LONG", price, conf, "Liquidity sweep LOW + RSI exhaustion"))

        # ===== REVERSAL SHORT =====
        if ctx["sweep_high"] and ctx["rsi"] > 65 and ctx["vol_climax"]:
            conf = 0.58
            conf += 0.05 if ctx["compressed"] else 0
            conf += 0.04 if ctx["htf_15m"] == "DOWN" else 0
            signals.append(self._build_signal(symbol, "SHORT", price, conf, "Liquidity sweep HIGH + RSI exhaustion"))

        # ===== PULLBACK & TREND =====
        ema9 = self.EMA(df5.close, 9).iloc[-1]
        ema21 = self.EMA(df5.close, 21).iloc[-1]

        if not signals:
            if price > ema9 > ema21 and ctx["rsi"] > 50:
                signals.append(self._build_signal(symbol, "LONG", price, 0.52, "Fallback trend continuation"))
            if price < ema9 < ema21 and ctx["rsi"] < 50:
                signals.append(self._build_signal(symbol, "SHORT", price, 0.52, "Fallback trend continuation"))

        # ===== COOL-DOWN CHECK =====
        now = datetime.utcnow()
        final_signals = []
        for s in signals:
            last_time = self.last_signal_time.get(symbol + s["signal"], datetime.min)
            if now - last_time >= timedelta(minutes=self.cooldown_minutes):
                final_signals.append(s)
                self.last_signal_time[symbol + s["signal"]] = now

        return final_signals

    def _build_signal(self, symbol, side, price, confidence, reason):
        return {
            "symbol": symbol,
            "signal": side,
            "price": round(price,6),
            "confidence": round(min(max(confidence, self.min_confidence), self.max_confidence),2),
            "time": datetime.utcnow().isoformat(),
            "reason": reason
        }

    # ===================== BATCH ANALYSIS =====================
    def analyze_symbols(self, symbols):
        all_signals = []
        for s in symbols:
            sigs = self.generate_signals(s)
            if sigs: all_signals.extend(sigs)
        all_signals.sort(key=lambda x: x["confidence"], reverse=True)
        return all_signals