package com.bot.core;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Precision and size filters for one symbol, filled by the adapter from
 * {@code /fapi/v1/exchangeInfo} and never guessed — a hardcoded tick size is the classic cause of
 * silent {@code REJECTED} orders. {@link BigDecimal} throughout: quantising with {@code double}
 * yields values like {@code 0.30000000000000004}, which the exchange rejects on precision. Fields map
 * to Binance's own filters — PRICE_FILTER for {@code tickSize}/{@code minPrice}/{@code maxPrice},
 * LOT_SIZE for {@code stepSize}/{@code minQty}/{@code maxQty}, and the two below.
 *
 * @param marketMaxQty MARKET_LOT_SIZE.maxQty — a separate, usually smaller cap for MARKET orders
 * @param minNotional  MIN_NOTIONAL.notional — the price x quantity floor
 */
public record InstrumentFilters(
        String symbol,
        BigDecimal tickSize,
        BigDecimal minPrice,
        BigDecimal maxPrice,
        BigDecimal stepSize,
        BigDecimal minQty,
        BigDecimal maxQty,
        BigDecimal marketMaxQty,
        BigDecimal minNotional,
        int pricePrecision,
        int quantityPrecision) {

    public InstrumentFilters {
        Preconditions.notBlank(symbol, "symbol");
        requirePositive(tickSize, "tickSize");
        requirePositive(stepSize, "stepSize");
        requireNonNegative(minPrice, "minPrice");
        requirePositive(maxPrice, "maxPrice");
        requirePositive(minQty, "minQty");
        requirePositive(maxQty, "maxQty");
        requirePositive(marketMaxQty, "marketMaxQty");
        requireNonNegative(minNotional, "minNotional");
        Preconditions.require(pricePrecision >= 0, "pricePrecision must be >= 0");
        Preconditions.require(quantityPrecision >= 0, "quantityPrecision must be >= 0");
        Preconditions.require(maxPrice.compareTo(minPrice) > 0, "maxPrice must exceed minPrice");
        Preconditions.require(maxQty.compareTo(minQty) >= 0, "maxQty must be >= minQty");
        // A tick finer than the accepted decimal places is self-contradictory exchange data: every
        // tick-aligned price would be unrepresentable, so fail here rather than per order.
        Preconditions.require(pricePrecision >= Math.max(0, tickSize.stripTrailingZeros().scale()),
                "pricePrecision " + pricePrecision + " cannot represent tickSize " + tickSize);
        Preconditions.require(quantityPrecision >= Math.max(0, stepSize.stripTrailingZeros().scale()),
                "quantityPrecision " + quantityPrecision + " cannot represent stepSize " + stepSize);
    }

    private static void requirePositive(BigDecimal v, String name) {
        Objects.requireNonNull(v, name);
        if (v.signum() <= 0) throw new IllegalArgumentException(name + " must be positive, got " + v);
    }

    private static void requireNonNegative(BigDecimal v, String name) {
        Objects.requireNonNull(v, name);
        if (v.signum() < 0) throw new IllegalArgumentException(name + " must be >= 0, got " + v);
    }

    public BigDecimal quantizePrice(double price, RoundingMode mode) {
        Preconditions.positiveFinite(price, "price");
        return quantize(BigDecimal.valueOf(price), tickSize, mode).setScale(pricePrecision, RoundingMode.UNNECESSARY);
    }

    /**
     * Rounds a stop <i>towards</i> entry: the quantity came from the unrounded stop distance, so
     * shrinking it keeps {@code risk <= R} true. Rounding away from entry would quietly break it.
     */
    public BigDecimal quantizeStopPrice(Side side, double stopPrice) {
        return quantizePrice(stopPrice, side == Side.LONG ? RoundingMode.CEILING : RoundingMode.FLOOR);
    }

    /** Rounds a take-profit <i>towards</i> entry: a fraction of a tick given up so the leg fills. */
    public BigDecimal quantizeTakeProfitPrice(Side side, double takeProfitPrice) {
        return quantizePrice(takeProfitPrice, side == Side.LONG ? RoundingMode.FLOOR : RoundingMode.CEILING);
    }

    /** Never rounds up: that would exceed the computed risk. */
    public BigDecimal quantizeQuantityDown(double qty) {
        Preconditions.nonNegativeFinite(qty, "qty");
        return quantize(BigDecimal.valueOf(qty), stepSize, RoundingMode.FLOOR)
                .setScale(quantityPrecision, RoundingMode.DOWN);
    }

    private static BigDecimal quantize(BigDecimal value, BigDecimal step, RoundingMode mode) {
        return value.divide(step, 0, mode).multiply(step).stripTrailingZeros();
    }

    public boolean isPriceOnTick(BigDecimal price) {
        return price.remainder(tickSize).compareTo(BigDecimal.ZERO) == 0;
    }

    public boolean isQuantityOnStep(BigDecimal qty) {
        return qty.remainder(stepSize).compareTo(BigDecimal.ZERO) == 0;
    }

    public boolean isPriceInRange(BigDecimal price) {
        return price.compareTo(minPrice) >= 0 && price.compareTo(maxPrice) <= 0;
    }

    public boolean isQuantityInRange(BigDecimal qty, boolean marketOrder) {
        BigDecimal cap = marketOrder ? marketMaxQty.min(maxQty) : maxQty;
        return qty.compareTo(minQty) >= 0 && qty.compareTo(cap) <= 0;
    }

    public boolean meetsMinNotional(BigDecimal price, BigDecimal qty) {
        return price.multiply(qty).compareTo(minNotional) >= 0;
    }

    /** Smallest quantity that clears both {@link #minQty} and {@link #minNotional} at {@code price}. */
    public BigDecimal smallestTradableQuantity(double price) {
        Preconditions.positiveFinite(price, "price");
        BigDecimal byNotional = minNotional.divide(BigDecimal.valueOf(price), quantityPrecision + 8, RoundingMode.CEILING);
        BigDecimal raw = byNotional.max(minQty);
        // ceil to the step, so the result really is tradable rather than one step short of it
        return quantize(raw, stepSize, RoundingMode.CEILING).setScale(quantityPrecision, RoundingMode.CEILING);
    }

    /** For tests and the smoke run only — the adapter always overwrites these with the real ones. */
    public static InstrumentFilters of(String symbol, String tickSize, String stepSize, String minNotional) {
        BigDecimal tick = new BigDecimal(tickSize);
        BigDecimal step = new BigDecimal(stepSize);
        return new InstrumentFilters(
                symbol,
                tick,
                tick,
                new BigDecimal("1000000000"),
                step,
                step,
                new BigDecimal("1000000"),
                new BigDecimal("1000000"),
                new BigDecimal(minNotional),
                Math.max(0, tick.stripTrailingZeros().scale()),
                Math.max(0, step.stripTrailingZeros().scale()));
    }
}
