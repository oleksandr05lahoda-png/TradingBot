import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://fapi.binance.com"

class Elite5MinAnalyzer:
    """Professional 5-minute futures analyzer optimized for safe futures entries"""

    def __init__(self, min_conf=0.45, max_conf=0.95, cooldown_minutes=3, max_threads=20):
        self.min_conf = min_conf
        self.max_conf = max_conf
        self.cooldown_minutes = cooldown_minutes
        self.last_signal_time = {}
        self.history = {}  # symbol streaks for adaptive confidence
        self.max_threads = max_threads

    # ===================== FETCH =====================
    def fetch_klines(self, symbol, interval="5m", limit=500):
        url = f"{BASE_URL}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
        data = requests.get(url, timeout=10).json()
        df = pd.DataFrame(data, columns=[
            "open_time","open","high","low","close","volume","close_time",
            "qav","trades","tb","tq","ignore"
        ])
        df = df.astype({"open":"float","high":"float","low":"float","close":"float","volume":"float"})
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
        ctx = {}
        price = df5.close.iloc[-1]
        atr = self.ATR(df5).iloc[-1]

        ema9_slope = self.EMA(df5.close, 9).iloc[-1] - self.EMA(df5.close, 9).iloc[-5]
        ema21_slope = self.EMA(df5.close, 21).iloc[-1] - self.EMA(df5.close, 21).iloc[-5]

        ctx.update({
            "price": price,
            "atr": atr,
            "ema9_slope": ema9_slope,
            "ema21_slope": ema21_slope,
            "rsi": self.RSI(df5.close).iloc[-1],
            "compressed": (df5.high.iloc[-1]-df5.low.iloc[-1]) < (df5.high-df5.low).rolling(20).mean().iloc[-1]*0.7,
            "vol_climax": df5.volume.iloc[-1] > df5.volume.rolling(30).mean().iloc[-1]*1.5
        })

        # Macro trends
        ctx["htf_15m"] = "UP" if self.EMA(df15.close, 9).iloc[-1] > self.EMA(df15.close, 21).iloc[-1] else "DOWN"
        ctx["htf_1h"] = "UP" if self.EMA(df1h.close, 9).iloc[-1] > self.EMA(df1h.close, 21).iloc[-1] else "DOWN"
        ctx["htf_4h"] = "UP" if self.EMA(df4h.close, 9).iloc[-1] > self.EMA(df4h.close, 21).iloc[-1] else "DOWN"

        # Liquidity sweeps
        ctx["sweep_high"] = df5.high.iloc[-1] > df5.high.iloc[-20:-1].max() and (df5.high.iloc[-1]-df5.close.iloc[-1]) > atr*0.2
        ctx["sweep_low"] = df5.low.iloc[-1] < df5.low.iloc[-20:-1].min() and (df5.close.iloc[-1]-df5.low.iloc[-1]) > atr*0.2

        # Micro trend exhaustion
        ctx["micro_trend_up"] = ema9_slope > 0 and ema21_slope > 0
        ctx["micro_trend_down"] = ema9_slope < 0 and ema21_slope < 0
        ctx["exhaustion_up"] = ctx["rsi"] > 72 and ctx["micro_trend_up"]
        ctx["exhaustion_down"] = ctx["rsi"] < 28 and ctx["micro_trend_down"]

        return ctx

    # ===================== SIGNAL GENERATION =====================
    def generate_signal(self, symbol):
        try:
            df5 = self.fetch_klines(symbol, "5m", 500)
            df15 = self.fetch_klines(symbol, "15m", 200)
            df1h = self.fetch_klines(symbol, "1h", 200)
            df4h = self.fetch_klines(symbol, "4h", 100)
        except Exception:
            return []

        if len(df5) < 120:
            return []

        ctx = self.build_context(df5, df15, df1h, df4h)
        price = ctx["price"]
        atr = ctx["atr"]
        signals = []

        # ===== REVERSAL LONG =====
        if ctx["sweep_low"] and ctx["rsi"] < 35 and ctx["vol_climax"] and ctx["htf_1h"]=="UP":
            conf = self.adaptive_conf(symbol, 0.58 + (0.05 if ctx["compressed"] else 0))
            signals.append(self._build_signal(symbol, "LONG", price, atr, conf, "REVERSAL LOW + HTF UP"))

        # ===== REVERSAL SHORT =====
        if ctx["sweep_high"] and ctx["rsi"] > 65 and ctx["vol_climax"] and ctx["htf_1h"]=="DOWN":
            conf = self.adaptive_conf(symbol, 0.58 + (0.05 if ctx["compressed"] else 0))
            signals.append(self._build_signal(symbol, "SHORT", price, atr, conf, "REVERSAL HIGH + HTF DOWN"))

        # ===== TREND fallback =====
        ema9 = self.EMA(df5.close, 9).iloc[-1]
        ema21 = self.EMA(df5.close, 21).iloc[-1]

        if not signals:
            if price > ema9 > ema21 and not ctx["exhaustion_up"]:
                conf = self.adaptive_conf(symbol, 0.52 + (0.03 if ctx["micro_trend_up"] else 0))
                signals.append(self._build_signal(symbol, "LONG", price, atr, conf, "TREND CONTINUATION"))

            if price < ema9 < ema21 and not ctx["exhaustion_down"]:
                conf = self.adaptive_conf(symbol, 0.52 + (0.03 if ctx["micro_trend_down"] else 0))
                signals.append(self._build_signal(symbol, "SHORT", price, atr, conf, "TREND CONTINUATION"))

        # ===== COOLDOWN CHECK =====
        now = datetime.utcnow()
        final_signals = []
        for s in signals:
            last_time = self.last_signal_time.get(symbol + s["signal"], datetime.min)
            if now - last_time >= timedelta(minutes=self.cooldown_minutes):
                final_signals.append(s)
                self.last_signal_time[symbol + s["signal"]] = now

        return final_signals

    # ===================== BUILD SIGNAL =====================
    def _build_signal(self, symbol, side, price, atr, confidence, reason):
        stop_mult = 1.2
        take_mult = 2.5 + (confidence - 0.5)
        stop = price - stop_mult * atr if side=="LONG" else price + stop_mult * atr
        take = price + take_mult * atr if side=="LONG" else price - take_mult * atr

        return {
            "symbol": symbol,
            "signal": side,
            "price": round(price,6),
            "stop": round(stop,6),
            "take": round(take,6),
            "confidence": round(min(max(confidence,self.min_conf),self.max_conf),2),
            "time": datetime.utcnow().isoformat(),
            "reason": reason
        }

    # ===================== ADAPTIVE CONFIDENCE =====================
    def adaptive_conf(self, symbol, base_conf):
        streak = self.history.get(symbol, 0)
        if streak >= 2: base_conf += 0.03
        if streak <= -2: base_conf -= 0.03
        return min(max(base_conf, self.min_conf), self.max_conf)

    def register_result(self, symbol, win=True):
        self.history[symbol] = self.history.get(symbol,0) + (1 if win else -1)

    # ===================== BATCH ANALYSIS =====================
    def analyze_symbols(self, symbols):
        all_signals = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            results = executor.map(self.generate_signal, symbols)
        for r in results:
            if r: all_signals.extend(r)
        all_signals.sort(key=lambda x: x["confidence"], reverse=True)
        return all_signals
