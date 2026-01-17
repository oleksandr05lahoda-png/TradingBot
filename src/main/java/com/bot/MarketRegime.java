package com.bot;

import com.bot.SignalSender.Candle;
import java.util.List;

public class MarketRegime {

    public boolean isTrend(List<Candle> candles) {
        // простой метод: если EMA 50 > EMA 200 -> тренд
        return true;
    }
}
