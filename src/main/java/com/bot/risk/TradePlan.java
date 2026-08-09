package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.math.BigDecimal;
import java.util.List;

/**
 * An approved trade: direction, size, the exact prices that will be sent, and the stop the size was
 * derived from. The only constructor is package-private and reachable from {@link RiskEngine} alone,
 * so a plan without a stop is not a state the execution layer has to defend against. It carries both
 * the source {@link StopLoss} and {@link #stopPrice()}, the tick-aligned price actually sent; they
 * can differ by a tick, and every risk number here comes from the price that will be sent.
 */
public final class TradePlan {

    private final String signalId;
    private final String symbol;
    private final Side side;
    private final BigDecimal entryPrice;
    private final BigDecimal stopPrice;
    private final StopLoss stop;
    private final BigDecimal quantity;
    private final int leverage;
    private final double notionalUsd;
    private final double initialMarginUsd;
    private final double riskUsd;
    private final double riskFractionOfBalance;
    private final double liquidationPrice;
    private final double liquidationBufferFraction;
    private final List<TakeProfitPolicy.ProjectedLeg> takeProfits;
    private final InstrumentFilters filters;
    private final String sizingNote;

    TradePlan(String signalId,
              String symbol,
              Side side,
              BigDecimal entryPrice,
              BigDecimal stopPrice,
              StopLoss stop,
              BigDecimal quantity,
              int leverage,
              double notionalUsd,
              double initialMarginUsd,
              double riskUsd,
              double riskFractionOfBalance,
              double liquidationPrice,
              double liquidationBufferFraction,
              List<TakeProfitPolicy.ProjectedLeg> takeProfits,
              InstrumentFilters filters,
              String sizingNote) {

        this.signalId = Preconditions.notBlank(signalId, "signalId");
        this.symbol = Preconditions.notBlank(symbol, "symbol");
        this.side = Preconditions.notNull(side, "side");
        this.entryPrice = Preconditions.notNull(entryPrice, "entryPrice");
        this.stopPrice = Preconditions.notNull(stopPrice, "stopPrice");
        this.stop = Preconditions.notNull(stop, "stop");
        this.quantity = Preconditions.notNull(quantity, "quantity");
        this.leverage = leverage;
        this.notionalUsd = Preconditions.positiveFinite(notionalUsd, "notionalUsd");
        this.initialMarginUsd = Preconditions.positiveFinite(initialMarginUsd, "initialMarginUsd");
        this.riskUsd = Preconditions.positiveFinite(riskUsd, "riskUsd");
        this.riskFractionOfBalance = Preconditions.positiveFinite(riskFractionOfBalance, "riskFractionOfBalance");
        this.liquidationPrice = Preconditions.nonNegativeFinite(liquidationPrice, "liquidationPrice");
        this.liquidationBufferFraction =
                Preconditions.nonNegativeFinite(liquidationBufferFraction, "liquidationBufferFraction");
        this.takeProfits = List.copyOf(Preconditions.notNull(takeProfits, "takeProfits"));
        this.filters = Preconditions.notNull(filters, "filters");
        this.sizingNote = Preconditions.notNull(sizingNote, "sizingNote");

        // Invariants restated rather than trusted: RiskEngine is the only caller today, not forever.
        Preconditions.require(quantity.signum() > 0, "quantity must be positive");
        Preconditions.require(filters.isQuantityOnStep(quantity),
                "quantity " + quantity + " is not a multiple of stepSize " + filters.stepSize());
        Preconditions.require(filters.isPriceOnTick(entryPrice),
                "entry " + entryPrice + " is not a multiple of tickSize " + filters.tickSize());
        Preconditions.require(filters.isPriceOnTick(stopPrice),
                "stop " + stopPrice + " is not a multiple of tickSize " + filters.tickSize());
        Preconditions.require(side.isValidStopGeometry(entryPrice.doubleValue(), stopPrice.doubleValue()),
                "stop " + stopPrice + " is on the wrong side of entry " + entryPrice + " for " + side);
        Preconditions.require(leverage >= 1 && leverage <= RiskConstants.MAX_LEVERAGE,
                "leverage " + leverage + " is outside [1, " + RiskConstants.MAX_LEVERAGE + "]");
        Preconditions.require(riskFractionOfBalance <= RiskConstants.MAX_RISK_FRACTION_PER_TRADE + 1e-12,
                "plan risks " + riskFractionOfBalance + " of balance, above the hard cap "
                        + RiskConstants.MAX_RISK_FRACTION_PER_TRADE);
        Preconditions.require(liquidationBufferFraction >= RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION,
                "liquidation buffer " + liquidationBufferFraction + " is below the hard floor "
                        + RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION);
        for (TakeProfitPolicy.ProjectedLeg leg : this.takeProfits) {
            Preconditions.require(
                    side.isValidTakeProfitGeometry(entryPrice.doubleValue(), leg.price().doubleValue()),
                    "take-profit " + leg.price() + " is not beyond entry " + entryPrice + " for " + side);
        }
        BigDecimal tpTotal = this.takeProfits.stream()
                .map(TakeProfitPolicy.ProjectedLeg::quantity)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        Preconditions.require(tpTotal.compareTo(quantity) <= 0,
                "take-profit legs total " + tpTotal + ", more than the position " + quantity);
    }

    public String signalId() { return signalId; }
    public String symbol() { return symbol; }
    public Side side() { return side; }
    /** Tick-aligned entry; all risk numbers in this plan are computed against it. */
    public BigDecimal entryPrice() { return entryPrice; }
    public BigDecimal stopPrice() { return stopPrice; }
    /** The source stop, structural or ATR fallback. */
    public StopLoss stop() { return stop; }
    public BigDecimal quantity() { return quantity; }
    public int leverage() { return leverage; }
    public double notionalUsd() { return notionalUsd; }
    public double initialMarginUsd() { return initialMarginUsd; }
    /** Money between entry and stop for this exact size. */
    public double riskUsd() { return riskUsd; }
    public double riskFractionOfBalance() { return riskFractionOfBalance; }
    /** 0.0 means liquidation is not reachable above zero (only possible for a long at low leverage). */
    public double liquidationPrice() { return liquidationPrice; }
    public double liquidationBufferFraction() { return liquidationBufferFraction; }
    /** Reduce-only exits, in ascending R. May be empty when the lot is too small to split or close. */
    public List<TakeProfitPolicy.ProjectedLeg> takeProfits() { return takeProfits; }
    public InstrumentFilters filters() { return filters; }
    /** Which ceiling, if any, reduced the size below what the stop alone would have allowed. */
    public String sizingNote() { return sizingNote; }

    /** Distance from entry to stop in quote currency — one R. */
    public double rUsdPerUnit() {
        return Math.abs(entryPrice.doubleValue() - stopPrice.doubleValue());
    }

    @Override public String toString() {
        return String.format(
                "TradePlan[%s %s qty=%s @ %s stop=%s (%s) lev=%dx notional=$%.2f margin=$%.2f "
                        + "risk=$%.2f (%.3f%% of balance) liq=%.8g buffer=%.1f%% tp=%d | %s]",
                side, symbol, quantity.toPlainString(), entryPrice.toPlainString(), stopPrice.toPlainString(),
                stop.origin(), leverage, notionalUsd, initialMarginUsd, riskUsd, riskFractionOfBalance * 100,
                liquidationPrice, liquidationBufferFraction * 100, takeProfits.size(), sizingNote);
    }
}
