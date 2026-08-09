package com.bot.core;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Precision and size filters for one symbol, as published by the exchange.
 *
 * <p>Fetched, never guessed — a hardcoded tick size is the classic cause of silent {@code REJECTED}
 * orders. Pure (no I/O), so the risk core can depend on it while the adapter fills it from
 * {@code /fapi/v1/exchangeInfo}.
 *
 * <p>{@link BigDecimal} throughout: quantising with {@code double} produces values like
 * {@code 0.30000000000000004}, which the exchange rejects on precision.
 *
 * <p>Quantity always rounds <b>down</b> — rounding up would exceed the computed risk. Prices round
 * in the caller's direction; {@link #quantizeStopPrice} and {@link #quantizeTakeProfitPrice} both
 * round <i>towards</i> entry, so realised risk can only be smaller than planned.
 *
 * <p>Fields map to Binance's own filters: {@code tickSize}/{@code minPrice}/{@code maxPrice} from
 * PRICE_FILTER, {@code stepSize}/{@code minQty}/{@code maxQty} from LOT_SIZE, and the two below.
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
        // tick-aligned price would then be unrepresentable. Fail here rather than emit orders that
        // are rejected for precision one at a time.
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

    // ─── Quantisation ────────────────────────────────────────────────────────────────────────

    /** Rounds {@code price} to a multiple of {@link #tickSize} in the given direction. */
    public BigDecimal quantizePrice(double price, RoundingMode mode) {
        Preconditions.positiveFinite(price, "price");
        return quantize(BigDecimal.valueOf(price), tickSize, mode).setScale(pricePrecision, RoundingMode.UNNECESSARY);
    }

    /**
     * Rounds a stop <i>towards</i> the entry. The sizer derived the quantity from the unrounded stop
     * distance; shrinking that distance can only shrink the money at risk, so this direction keeps
     * the invariant {@code risk <= R} true after quantisation. Rounding away from entry would
     * quietly break it.
     */
    public BigDecimal quantizeStopPrice(Side side, double stopPrice) {
        return quantizePrice(stopPrice, side == Side.LONG ? RoundingMode.CEILING : RoundingMode.FLOOR);
    }

    /**
     * Rounds a take-profit <i>towards</i> the entry, i.e. to the nearer, more reachable tick.
     * The cost is a fraction of a tick of profit; the benefit is that the leg actually fills.
     */
    public BigDecimal quantizeTakeProfitPrice(Side side, double takeProfitPrice) {
        return quantizePrice(takeProfitPrice, side == Side.LONG ? RoundingMode.FLOOR : RoundingMode.CEILING);
    }

    /** Rounds {@code qty} DOWN to a multiple of {@link #stepSize}. Never rounds up — see class javadoc. */
    public BigDecimal quantizeQuantityDown(double qty) {
        Preconditions.nonNegativeFinite(qty, "qty");
        return quantize(BigDecimal.valueOf(qty), stepSize, RoundingMode.FLOOR)
                .setScale(quantityPrecision, RoundingMode.DOWN);
    }

    private static BigDecimal quantize(BigDecimal value, BigDecimal step, RoundingMode mode) {
        return value.divide(step, 0, mode).multiply(step).stripTrailingZeros();
    }

    // ─── Validation, run BEFORE the order is sent ────────────────────────────────────────────

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

    /**
     * Filters that are convenient in tests and in the smoke run when the exchange has not been
     * queried yet. NOT a substitute for the real ones — the adapter always overwrites these.
     */
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
