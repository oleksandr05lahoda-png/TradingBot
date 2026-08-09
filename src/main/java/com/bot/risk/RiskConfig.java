package com.bot.risk;

import com.bot.core.Preconditions;

/**
 * Everything about the risk core that is allowed to vary, bounded by {@link RiskConstants}.
 *
 * <p>Every ceiling here is a <b>share of capital</b>, not a dollar amount. A fixed dollar cap stops
 * meaning anything the moment the balance moves: $500 of notional is a tenth of one account and
 * five times another, and the version of that mistake this repository already made was capping
 * position <i>count</i> instead of exposure — twenty correlated alts at 20% each is 400% of the
 * account and reads as "one position at a time" on the dashboard. The one absolute value,
 * {@link #maxNotionalUsdPerTrade}, is an extra ceiling on top of the fractional one, never a
 * replacement for it, and defaults to unbounded.
 *
 * <p>Constructing an instance that breaks a hard constant is impossible: the compact constructor
 * refuses. There is no environment variable that widens {@link RiskConstants#MAX_LEVERAGE},
 * {@link RiskConstants#MAX_RISK_FRACTION_PER_TRADE} or
 * {@link RiskConstants#MIN_LIQUIDATION_BUFFER_FRACTION}, and a config may only be stricter.
 *
 * @param riskFractionPerTrade        fraction of balance risked per trade, capped at 1%
 * @param maxLeverage                 leverage ceiling, capped at {@link RiskConstants#MAX_LEVERAGE}
 * @param maxNotionalFractionPerTrade per-trade notional as a fraction of balance
 * @param maxNotionalUsdPerTrade      absolute per-trade notional ceiling; {@code +Infinity} = unbounded
 * @param maxLongExposureFraction     sum of LONG notional as a fraction of balance
 * @param maxShortExposureFraction    sum of SHORT notional as a fraction of balance
 * @param maxConcurrentPositions      how many symbols may be open at once
 * @param dailyLossFractionLimit      realised + open drawdown that trips the kill switch for the UTC day
 * @param minLiquidationBufferFraction may be raised above 30%, never lowered
 * @param maxMarginUtilizationFraction initial margin of a new position as a fraction of balance
 * @param takerFeeFraction            used to discount isolated margin when projecting liquidation
 * @param atrStopMultiplier           multiplier applied to ATR when no structural stop arrived
 * @param takeProfitPolicy            where the reduce-only exits sit, in R
 */
public record RiskConfig(
        double riskFractionPerTrade,
        int maxLeverage,
        double maxNotionalFractionPerTrade,
        double maxNotionalUsdPerTrade,
        double maxLongExposureFraction,
        double maxShortExposureFraction,
        int maxConcurrentPositions,
        double dailyLossFractionLimit,
        double minLiquidationBufferFraction,
        double maxMarginUtilizationFraction,
        double takerFeeFraction,
        double atrStopMultiplier,
        TakeProfitPolicy takeProfitPolicy) {

    public RiskConfig {
        Preconditions.require(riskFractionPerTrade > 0, "riskFractionPerTrade must be positive");
        Preconditions.require(riskFractionPerTrade <= RiskConstants.MAX_RISK_FRACTION_PER_TRADE,
                "riskFractionPerTrade " + riskFractionPerTrade + " exceeds the hard cap "
                        + RiskConstants.MAX_RISK_FRACTION_PER_TRADE);
        Preconditions.positive(maxLeverage, "maxLeverage");
        Preconditions.require(maxLeverage <= RiskConstants.MAX_LEVERAGE,
                "maxLeverage " + maxLeverage + " exceeds the hard cap " + RiskConstants.MAX_LEVERAGE);
        Preconditions.require(maxNotionalFractionPerTrade > 0, "maxNotionalFractionPerTrade must be positive");
        Preconditions.require(maxNotionalUsdPerTrade > 0, "maxNotionalUsdPerTrade must be positive");
        Preconditions.require(maxLongExposureFraction > 0, "maxLongExposureFraction must be positive");
        Preconditions.require(maxShortExposureFraction > 0, "maxShortExposureFraction must be positive");
        Preconditions.positive(maxConcurrentPositions, "maxConcurrentPositions");
        Preconditions.require(dailyLossFractionLimit > 0 && dailyLossFractionLimit < 1.0,
                "dailyLossFractionLimit must be in (0, 1), got " + dailyLossFractionLimit);
        Preconditions.require(minLiquidationBufferFraction >= RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION,
                "minLiquidationBufferFraction " + minLiquidationBufferFraction
                        + " is below the hard floor " + RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION);
        Preconditions.require(minLiquidationBufferFraction < 1.0,
                "minLiquidationBufferFraction must be below 1.0");
        Preconditions.require(maxMarginUtilizationFraction > 0 && maxMarginUtilizationFraction <= 1.0,
                "maxMarginUtilizationFraction must be in (0, 1]");
        Preconditions.inClosedRange(takerFeeFraction, 0.0, 0.01, "takerFeeFraction");
        Preconditions.positiveFinite(atrStopMultiplier, "atrStopMultiplier");
        Preconditions.notNull(takeProfitPolicy, "takeProfitPolicy");
    }

    /**
     * Starting values for a small testnet account. Deliberately tight; the point of the exercise is
     * that the machinery refuses correctly, not that it trades often.
     */
    public static RiskConfig defaults() {
        return new RiskConfig(
                RiskConstants.DEFAULT_RISK_FRACTION_PER_TRADE,
                RiskConstants.MAX_LEVERAGE,
                1.0,                       // one trade may carry notional up to 100% of balance
                Double.POSITIVE_INFINITY,  // no separate absolute ceiling by default
                2.0,                       // aggregate LONG notional up to 200% of balance
                2.0,                       // aggregate SHORT notional up to 200% of balance
                3,
                0.03,                      // 3% of the day's starting balance stops the day
                RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION,
                0.50,                      // a new position may lock up at most half the balance as margin
                RiskConstants.DEFAULT_TAKER_FEE_FRACTION,
                2.0,
                TakeProfitPolicy.standard());
    }

    public RiskConfig withRiskFractionPerTrade(double v) {
        return new RiskConfig(v, maxLeverage, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                maxLongExposureFraction, maxShortExposureFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, takeProfitPolicy);
    }

    public RiskConfig withMaxLeverage(int v) {
        return new RiskConfig(riskFractionPerTrade, v, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                maxLongExposureFraction, maxShortExposureFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, takeProfitPolicy);
    }

    public RiskConfig withMaxConcurrentPositions(int v) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                maxLongExposureFraction, maxShortExposureFraction, v,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, takeProfitPolicy);
    }

    public RiskConfig withExposureFractions(double longFraction, double shortFraction) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                longFraction, shortFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, takeProfitPolicy);
    }

    public RiskConfig withMaxNotionalPerTrade(double fractionOfBalance, double absoluteUsd) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, fractionOfBalance, absoluteUsd,
                maxLongExposureFraction, maxShortExposureFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, takeProfitPolicy);
    }

    public RiskConfig withDailyLossFractionLimit(double v) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                maxLongExposureFraction, maxShortExposureFraction, maxConcurrentPositions,
                v, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, takeProfitPolicy);
    }

    public RiskConfig withTakeProfitPolicy(TakeProfitPolicy v) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                maxLongExposureFraction, maxShortExposureFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, v);
    }
}
