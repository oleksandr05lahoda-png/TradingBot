package com.bot.risk;

/**
 * Why a trade was refused. Enumerated, not free text, so refusals can be counted — a gate that
 * rejects everything for one reason is a bug, and invisible if every refusal is a string.
 */
public enum RejectReason {

    /** An input could not be validated. Fail-closed: unknown is never treated as zero or as fine. */
    INVALID_INPUT,

    TRADING_HALTED,

    /** Above the configured ceiling, which cannot itself exceed {@link RiskConstants#MAX_LEVERAGE}. */
    LEVERAGE_ABOVE_MAX,

    LEVERAGE_ABOVE_EXCHANGE_BRACKET,

    /** No structural stop and no ATR: nothing to size from, so nothing to trade. */
    NO_STOP_AVAILABLE,

    /** The stop is not on the losing side of the entry, or tick alignment collapsed the distance. */
    STOP_GEOMETRY,

    /** Entry or stop falls outside the symbol's PRICE_FILTER range. */
    PRICE_OUT_OF_RANGE,

    POSITION_ALREADY_OPEN,

    MAX_CONCURRENT_POSITIONS,

    LONG_EXPOSURE_CAP,

    SHORT_EXPOSURE_CAP,

    BELOW_MIN_QUANTITY,

    BELOW_MIN_NOTIONAL,

    /** Initial margin exceeds the share of balance the config permits to be locked up. */
    INSUFFICIENT_MARGIN,

    /** The stop is not far enough inside the liquidation price. See {@link LiquidationSafety}. */
    LIQUIDATION_BUFFER,

    /** The final size would risk more than the budget; unreachable, checked anyway. */
    RISK_BUDGET_OVERRUN
}
