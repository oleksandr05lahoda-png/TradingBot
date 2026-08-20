package com.bot.risk;

import com.bot.core.Preconditions;

/**
 * Everything about the risk core that may vary, bounded by {@link RiskConstants}: nothing here, and
 * no environment variable, can widen MAX_LEVERAGE, MAX_RISK_FRACTION_PER_TRADE or
 * MIN_LIQUIDATION_BUFFER_FRACTION. Every {@code ...Fraction} is a share of balance, not dollars — an
 * earlier version capped position COUNT, and twenty correlated alts at 20% each is 400% of account.
 *
 * @param maxNotionalUsdPerTrade       extra ceiling on the fractional one, not a replacement
 * @param minLiquidationBufferFraction may be raised above 30%, never lowered
 * @param targetDailyVolFraction       a fraction of price, not of balance; see {@link VolTargetOverlay}
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
        double targetDailyVolFraction,
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
        // A target of 100%+ makes the overlay decorative; well before that it is a typo.
        Preconditions.require(targetDailyVolFraction > 0 && targetDailyVolFraction < 1.0,
                "targetDailyVolFraction must be in (0, 1), got " + targetDailyVolFraction);
        Preconditions.notNull(takeProfitPolicy, "takeProfitPolicy");
    }

    /** Starting values for a small account, deliberately tight. */
    public static RiskConfig defaults() {
        return new RiskConfig(
                RiskConstants.DEFAULT_RISK_FRACTION_PER_TRADE,
                RiskConstants.MAX_LEVERAGE,
                1.0,                       // notional up to 100% of balance per trade
                Double.POSITIVE_INFINITY,  // no absolute ceiling
                2.0,                       // aggregate LONG up to 200% of balance
                2.0,                       // aggregate SHORT up to 200% of balance
                3,
                0.03,                      // 3% of the day's starting balance stops the day
                RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION,
                0.50,                      // at most half the balance locked as margin
                RiskConstants.DEFAULT_TAKER_FEE_FRACTION,
                2.0,
                RiskConstants.DEFAULT_TARGET_DAILY_VOL_FRACTION,
                TakeProfitPolicy.standard());
    }

    public RiskConfig withMaxLeverage(int v) {
        return new RiskConfig(riskFractionPerTrade, v, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                maxLongExposureFraction, maxShortExposureFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, targetDailyVolFraction, takeProfitPolicy);
    }

    public RiskConfig withMaxConcurrentPositions(int v) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                maxLongExposureFraction, maxShortExposureFraction, v,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, targetDailyVolFraction, takeProfitPolicy);
    }

    public RiskConfig withExposureFractions(double longFraction, double shortFraction) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                longFraction, shortFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, targetDailyVolFraction, takeProfitPolicy);
    }

    public RiskConfig withMaxNotionalPerTrade(double fractionOfBalance, double absoluteUsd) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, fractionOfBalance, absoluteUsd,
                maxLongExposureFraction, maxShortExposureFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, targetDailyVolFraction, takeProfitPolicy);
    }

    public RiskConfig withTakeProfitPolicy(TakeProfitPolicy v) {
        return new RiskConfig(riskFractionPerTrade, maxLeverage, maxNotionalFractionPerTrade, maxNotionalUsdPerTrade,
                maxLongExposureFraction, maxShortExposureFraction, maxConcurrentPositions,
                dailyLossFractionLimit, minLiquidationBufferFraction, maxMarginUtilizationFraction,
                takerFeeFraction, atrStopMultiplier, targetDailyVolFraction, v);
    }
}
