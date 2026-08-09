package com.bot.risk;

/**
 * Why a trade was refused. Enumerated rather than free text so refusals can be counted: a gate that
 * rejects everything for one reason is a bug, and it is invisible if every refusal is a string.
 */
public enum RejectReason {

    /** An input could not be validated. Fail-closed: unknown is never treated as zero or as fine. */
    INVALID_INPUT,

    /** The daily loss kill switch is latched, or the operator stopped trading. */
    TRADING_HALTED,

    /** Requested leverage is above the configured (and therefore the hard) ceiling. */
    LEVERAGE_ABOVE_MAX,

    /** Requested leverage is above what the exchange itself permits at this notional. */
    LEVERAGE_ABOVE_EXCHANGE_BRACKET,

    /** No structural stop and no ATR: nothing to size from, so nothing to trade. */
    NO_STOP_AVAILABLE,

    /** The stop is not on the losing side of the entry, or tick alignment collapsed the distance. */
    STOP_GEOMETRY,

    /** Entry or stop falls outside the symbol's PRICE_FILTER range. */
    PRICE_OUT_OF_RANGE,

    /** A position on this symbol is already open. */
    POSITION_ALREADY_OPEN,

    /** The concurrent position limit is reached. */
    MAX_CONCURRENT_POSITIONS,

    /** Aggregate LONG notional would exceed its cap. */
    LONG_EXPOSURE_CAP,

    /** Aggregate SHORT notional would exceed its cap. */
    SHORT_EXPOSURE_CAP,

    /** After every ceiling was applied, the remaining size is below the exchange's minimum lot. */
    BELOW_MIN_QUANTITY,

    /** After every ceiling was applied, the remaining size is below the exchange's minimum notional. */
    BELOW_MIN_NOTIONAL,

    /** Initial margin for this position exceeds the share of balance the config permits to be locked. */
    INSUFFICIENT_MARGIN,

    /** The stop is not far enough inside the liquidation price. See {@link LiquidationSafety}. */
    LIQUIDATION_BUFFER,

    /**
     * The final size would risk more than the budget. Unreachable if the arithmetic above is right,
     * which is exactly why it is checked: the last line of defence has to assume the earlier ones failed.
     */
    RISK_BUDGET_OVERRUN
}
