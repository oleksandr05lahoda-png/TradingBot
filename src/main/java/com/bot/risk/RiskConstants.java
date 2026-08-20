package com.bot.risk;

/**
 * Hard ceilings, {@code static final} on purpose: a limit an environment variable can move is a
 * suggestion. Configurable values live in {@link RiskConfig}, which cannot be built exceeding these.
 */
public final class RiskConstants {

    private RiskConstants() {}

    /** Absolute maximum leverage, enforced everywhere leverage is accepted. Not overridable. */
    public static final int MAX_LEVERAGE = 5;

    /** Risk per trade as a fraction of balance when the caller does not specify one: 0.5%. */
    public static final double DEFAULT_RISK_FRACTION_PER_TRADE = 0.005;

    /** Ceiling on risk per trade as a fraction of balance: 1%. {@link RiskConfig} refuses more. */
    public static final double MAX_RISK_FRACTION_PER_TRADE = 0.01;

    /** {@code |stop-liq| >= 0.30*|entry-liq|}, stop strictly between; rationale in {@link LiquidationSafety}. */
    public static final double MIN_LIQUIDATION_BUFFER_FRACTION = 0.30;

    /** Binance USDⓈ-M taker fee at VIP 0; discounts isolated margin when projecting liquidation. */
    public static final double DEFAULT_TAKER_FEE_FRACTION = 0.0005;

    /** Daily vol the {@link VolTargetOverlay} aims at: 2%, the calm end of BTC's ~1.5-5% range. */
    public static final double DEFAULT_TARGET_DAILY_VOL_FRACTION = 0.02;
}
