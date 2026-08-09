package com.bot.risk;

/**
 * Hard ceilings of the risk core. These are {@code static final} on purpose: a limit that can be
 * moved by an environment variable is not a limit, it is a suggestion, and the one that matters
 * gets raised at exactly the wrong moment.
 *
 * <p>Anything configurable lives in {@link RiskConfig}, and {@link RiskConfig} is itself bounded by
 * the constants here — it cannot be built with values that exceed them.
 */
public final class RiskConstants {

    private RiskConstants() {}

    /**
     * Absolute maximum leverage, enforced everywhere leverage is accepted. Not overridable.
     * Covered by {@code MaxLeverageTest}.
     */
    public static final int MAX_LEVERAGE = 5;

    /** Risk per trade as a fraction of balance when the caller does not specify one: 0.5%. */
    public static final double DEFAULT_RISK_FRACTION_PER_TRADE = 0.005;

    /** Ceiling on risk per trade as a fraction of balance: 1%. {@link RiskConfig} refuses more. */
    public static final double MAX_RISK_FRACTION_PER_TRADE = 0.01;

    /**
     * The stop must leave at least this fraction of the entry-to-liquidation distance unused.
     * Formally: {@code |stop - liq| >= 0.30 * |entry - liq|}, with the stop strictly between entry
     * and liquidation. A stop that sits closer to liquidation than this is not a stop — a wick,
     * a funding tick or a mark-price excursion closes the position at the exchange's price rather
     * than yours, and the loss is the whole isolated margin instead of the planned R.
     */
    public static final double MIN_LIQUIDATION_BUFFER_FRACTION = 0.30;

    /**
     * Binance USDⓈ-M taker fee for the default VIP 0 tier, as a fraction of notional. Used to
     * discount the isolated wallet balance when projecting the liquidation price, because the entry
     * fee is taken out of that same isolated margin — ignoring it places the liquidation price
     * further away than it really is, which is the dangerous direction to be wrong in.
     */
    public static final double DEFAULT_TAKER_FEE_FRACTION = 0.0005;
}
