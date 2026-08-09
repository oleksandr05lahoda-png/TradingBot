package com.bot.risk;

/**
 * Hard ceilings of the risk core, {@code static final} on purpose: a limit an environment variable
 * can move is a suggestion. Configurable values live in {@link RiskConfig}, which cannot be built
 * with anything exceeding the constants here.
 */
public final class RiskConstants {

    private RiskConstants() {}

    /** Absolute maximum leverage, enforced everywhere leverage is accepted. Not overridable. */
    public static final int MAX_LEVERAGE = 5;

    /** Risk per trade as a fraction of balance when the caller does not specify one: 0.5%. */
    public static final double DEFAULT_RISK_FRACTION_PER_TRADE = 0.005;

    /** Ceiling on risk per trade as a fraction of balance: 1%. {@link RiskConfig} refuses more. */
    public static final double MAX_RISK_FRACTION_PER_TRADE = 0.01;

    /**
     * {@code |stop - liq| >= 0.30 * |entry - liq|}, with the stop strictly between entry and
     * liquidation. Rationale in {@link LiquidationSafety}, which enforces it.
     */
    public static final double MIN_LIQUIDATION_BUFFER_FRACTION = 0.30;

    /**
     * Binance USDⓈ-M taker fee at VIP 0, as a fraction of notional. Used to discount the isolated
     * wallet balance when projecting liquidation — the entry fee comes out of that same margin.
     */
    public static final double DEFAULT_TAKER_FEE_FRACTION = 0.0005;
}
