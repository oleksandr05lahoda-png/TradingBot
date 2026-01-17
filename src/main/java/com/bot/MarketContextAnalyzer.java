package com.bot;

import com.bot.SignalSender.Candle;
import com.bot.SignalSender.MicroTrendResult;
import java.util.List;

public class MarketContextAnalyzer {

    public static MarketContext analyze(List<Candle> candles5m, List<Candle> candles15m) {
        double atr5 = SignalSender.atr(candles5m, 14);
        int struct5 = SignalSender.marketStructure(candles5m);
        int struct15 = SignalSender.marketStructure(candles15m);
        boolean bos = SignalSender.detectBOS(candles5m);
        boolean lsweep = SignalSender.detectLiquiditySweep(candles5m);

        return new MarketContext(atr5, struct5, struct15, bos, lsweep);
    }

    public static class MarketContext {
        public final double atr;
        public final int struct5;
        public final int struct15;
        public final boolean bos;
        public final boolean liquiditySweep;

        public MarketContext(double atr, int struct5, int struct15, boolean bos, boolean liquiditySweep) {
            this.atr = atr;
            this.struct5 = struct5;
            this.struct15 = struct15;
            this.bos = bos;
            this.liquiditySweep = liquiditySweep;
        }
    }
}
