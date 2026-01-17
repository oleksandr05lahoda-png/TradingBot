package com.bot;

import com.bot.SignalSender.Candle;
import java.util.List;

public class Backtester {

    public static class Result {
        public double netProfit;
        public double maxDrawdown;
        public double winRate;
        public int trades;
    }

    public Result run(List<Candle> candles5m, List<Candle> candles15m) {
        Result res = new Result();
        // ТУТ нужно реализовать прогон стратегии по свечам
        // Можно просто: каждый бар -> evaluate() -> если сигнал -> считать PnL
        return res;
    }
}
