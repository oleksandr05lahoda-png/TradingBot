package com.bot.risk;

import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What the bot believes is open. A cache, not truth: the exchange decides, and the reconciler
 * overwrites this via {@link #replaceAll}. Long and short are tracked separately and never netted —
 * a 100k long plus a 100k short is two positions with two liquidation prices, not a flat book.
 */
public final class ExposureBook {

    /** {@code quantity} is always positive — direction lives in {@code side}; {@code riskUsd} is
     *  the entry-to-stop money as planned at open. */
    public record OpenPosition(
            String symbol,
            Side side,
            BigDecimal quantity,
            double entryPrice,
            double notionalUsd,
            double riskUsd) {

        public OpenPosition {
            Preconditions.notBlank(symbol, "symbol");
            Preconditions.notNull(side, "side");
            Preconditions.notNull(quantity, "quantity");
            Preconditions.require(quantity.signum() > 0, "quantity must be positive, got " + quantity);
            Preconditions.positiveFinite(entryPrice, "entryPrice");
            Preconditions.positiveFinite(notionalUsd, "notionalUsd");
            Preconditions.nonNegativeFinite(riskUsd, "riskUsd");
        }
    }

    private final Map<String, OpenPosition> positions = new ConcurrentHashMap<>();

    public Optional<OpenPosition> get(String symbol) {
        return Optional.ofNullable(positions.get(symbol));
    }

    public boolean hasPosition(String symbol) {
        return positions.containsKey(symbol);
    }

    public int openCount() {
        return positions.size();
    }

    public List<OpenPosition> all() {
        return List.copyOf(positions.values());
    }

    public double exposureUsd(Side side) {
        double sum = 0;
        for (OpenPosition p : positions.values()) {
            if (p.side() == side) sum += p.notionalUsd();
        }
        return sum;
    }

    public double longExposureUsd() { return exposureUsd(Side.LONG); }

    public double shortExposureUsd() { return exposureUsd(Side.SHORT); }

    /** Total money at risk across open positions, if every stop filled at its trigger. */
    public double totalRiskUsd() {
        double sum = 0;
        for (OpenPosition p : positions.values()) sum += p.riskUsd();
        return sum;
    }

    /** Records a newly opened position. Refuses to shadow an existing one for the same symbol. */
    public void open(OpenPosition position) {
        Preconditions.notNull(position, "position");
        OpenPosition previous = positions.putIfAbsent(position.symbol(), position);
        Preconditions.require(previous == null,
                "a position on " + position.symbol() + " is already open; close it before opening another");
    }

    public Optional<OpenPosition> close(String symbol) {
        return Optional.ofNullable(positions.remove(symbol));
    }

    /** Overwrites the book from the exchange; reconciliation is the only caller allowed to do this. */
    public void replaceAll(Collection<OpenPosition> truth) {
        Preconditions.notNull(truth, "truth");
        Map<String, OpenPosition> next = new LinkedHashMap<>();
        for (OpenPosition p : truth) next.put(p.symbol(), p);
        positions.keySet().retainAll(next.keySet());
        positions.putAll(next);
    }

    @Override public String toString() {
        return String.format("ExposureBook[n=%d long=$%.2f short=$%.2f risk=$%.2f]",
                openCount(), longExposureUsd(), shortExposureUsd(), totalRiskUsd());
    }
}
