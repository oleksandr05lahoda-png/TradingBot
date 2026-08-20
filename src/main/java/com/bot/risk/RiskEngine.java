package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.logging.Logger;

/**
 * The gate: every position passes through {@link #evaluate}, and nothing that fails it can be built
 * downstream because {@link TradePlan} has no public constructor. Balance, filters, brackets and time
 * are arguments — no I/O, no clock — so a refusal is reproducible from its log line. Ceilings (step 8)
 * only ever reduce the sizing (step 7) derived from the stop.
 */
public final class RiskEngine {

    private static final Logger LOG = Logger.getLogger(RiskEngine.class.getName());

    private final RiskConfig config;
    private final ExposureBook book;
    private final DailyLossKillSwitch killSwitch;
    private final VolatilitySource volSource;

    /** No volatility feed: the vol-targeting overlay multiplies by 1.0 and sizing is unchanged. */
    public RiskEngine(RiskConfig config, ExposureBook book, DailyLossKillSwitch killSwitch) {
        this(config, book, killSwitch, VolatilitySource.none());
    }

    public RiskEngine(RiskConfig config, ExposureBook book, DailyLossKillSwitch killSwitch,
                      VolatilitySource volSource) {
        this.config = Preconditions.notNull(config, "config");
        this.book = Preconditions.notNull(book, "book");
        this.killSwitch = Preconditions.notNull(killSwitch, "killSwitch");
        this.volSource = Preconditions.notNull(volSource, "volSource");
    }

    public RiskConfig config() { return config; }
    public ExposureBook book() { return book; }
    public DailyLossKillSwitch killSwitch() { return killSwitch; }

    /** Decides whether {@code request} may become a position; {@code now} only sets the UTC day. */
    public RiskDecision evaluate(TradeRequest request, double balanceUsd, Instant now) {
        Preconditions.notNull(request, "request");
        Preconditions.notNull(now, "now");

        // 1 ─ Fail-closed inputs: an unreadable balance is not zero, and NaN is not a small number.
        if (!Double.isFinite(balanceUsd) || balanceUsd <= 0) {
            return RiskDecision.reject(RejectReason.INVALID_INPUT,
                    "balance is not a usable number: " + balanceUsd
                            + " — refusing rather than assuming a value");
        }

        // 2 ─ Kill switch. Latched for the rest of the UTC day once the daily loss limit is crossed.
        killSwitch.observeBalance(balanceUsd, now);
        DailyLossKillSwitch.Status halt = killSwitch.evaluate(now);
        if (halt.tripped()) {
            return RiskDecision.reject(RejectReason.TRADING_HALTED, halt.reason());
        }

        // 3 ─ Leverage. RiskConfig cannot exceed RiskConstants.MAX_LEVERAGE, so this covers both.
        if (request.leverage() > config.maxLeverage()) {
            return RiskDecision.reject(RejectReason.LEVERAGE_ABOVE_MAX,
                    "requested " + request.leverage() + "x, ceiling is " + config.maxLeverage()
                            + "x (hard limit " + RiskConstants.MAX_LEVERAGE + "x)");
        }

        // 4 ─ Position slots.
        if (book.hasPosition(request.symbol())) {
            return RiskDecision.reject(RejectReason.POSITION_ALREADY_OPEN,
                    "already holding " + request.symbol());
        }
        if (book.openCount() >= config.maxConcurrentPositions()) {
            return RiskDecision.reject(RejectReason.MAX_CONCURRENT_POSITIONS,
                    book.openCount() + " open, limit is " + config.maxConcurrentPositions());
        }

        // 5 ─ The stop. Structural if the signal carried one, ATR otherwise, refusal if neither.
        StopLoss stop;
        try {
            stop = StopLoss.resolve(request.side(), request.entryPrice(),
                    request.structuralStopPrice(), request.atr(), config.atrStopMultiplier());
        } catch (IllegalArgumentException e) {
            boolean nothingToUse = request.structuralStopPrice().isEmpty() && request.atr().isEmpty();
            return RiskDecision.reject(
                    nothingToUse ? RejectReason.NO_STOP_AVAILABLE : RejectReason.STOP_GEOMETRY,
                    e.getMessage());
        }

        // 6 ─ Tick alignment, then re-check the geometry: entry moves to a tick no worse than
        //     requested, the stop moves towards entry, so realised risk can only shrink.
        InstrumentFilters filters = request.filters();
        BigDecimal entryTick;
        BigDecimal stopTick;
        try {
            entryTick = filters.quantizePrice(request.entryPrice(),
                    request.side() == Side.LONG ? RoundingMode.FLOOR : RoundingMode.CEILING);
            stopTick = filters.quantizeStopPrice(request.side(), stop.price());
        } catch (ArithmeticException | IllegalArgumentException e) {
            return RiskDecision.reject(RejectReason.PRICE_OUT_OF_RANGE,
                    "entry/stop could not be aligned to tickSize " + filters.tickSize() + ": " + e.getMessage());
        }
        double entry = entryTick.doubleValue();
        double stopPrice = stopTick.doubleValue();

        if (!filters.isPriceInRange(entryTick) || !filters.isPriceInRange(stopTick)) {
            return RiskDecision.reject(RejectReason.PRICE_OUT_OF_RANGE,
                    "entry " + entryTick + " or stop " + stopTick + " is outside PRICE_FILTER ["
                            + filters.minPrice() + ", " + filters.maxPrice() + "]");
        }
        if (!request.side().isValidStopGeometry(entry, stopPrice)) {
            return RiskDecision.reject(RejectReason.STOP_GEOMETRY,
                    "after tick alignment the stop " + stopTick + " is no longer on the losing side of "
                            + entryTick + " for a " + request.side()
                            + " — the stop distance is smaller than one tick");
        }

        // 7 ─ Size from the stop, scaled by the vol-targeting overlay (Moreira-Muir): when realized
        //     vol runs above the target, the risk fraction shrinks by min(1, target/realized) BEFORE
        //     sizing, so every downstream number — budget, ceilings, margin — sees the reduced risk.
        //     With no vol data the multiplier is exactly 1.0 and this step is the pre-overlay bot.
        double volMultiplier = VolTargetOverlay.multiplier(
                config.targetDailyVolFraction(), realizedVolOrEmpty(request.symbol()));
        double effectiveRiskFraction = config.riskFractionPerTrade() * volMultiplier;
        double budgetedRiskUsd = balanceUsd * effectiveRiskFraction;
        double idealQty = PositionSizer.quantityForRisk(
                balanceUsd, effectiveRiskFraction, entry, stopPrice);

        // 8 ─ Ceilings, each a maximum notional; the tightest wins and the binding one is recorded.
        double perTradeCap = Math.min(balanceUsd * config.maxNotionalFractionPerTrade(),
                config.maxNotionalUsdPerTrade());
        double sideCapFraction = request.side() == Side.LONG
                ? config.maxLongExposureFraction() : config.maxShortExposureFraction();
        double sideUsed = book.exposureUsd(request.side());
        double sideHeadroom = balanceUsd * sideCapFraction - sideUsed;
        if (sideHeadroom <= 0) {
            return RiskDecision.reject(
                    request.side() == Side.LONG ? RejectReason.LONG_EXPOSURE_CAP : RejectReason.SHORT_EXPOSURE_CAP,
                    String.format("%s exposure $%.2f already at the cap %.0f%% of $%.2f",
                            request.side(), sideUsed, sideCapFraction * 100, balanceUsd));
        }
        double marginCap = balanceUsd * config.maxMarginUtilizationFraction() * request.leverage();
        double lotCap = filters.marketMaxQty().min(filters.maxQty()).doubleValue() * entry;

        double notionalCap = Math.min(Math.min(perTradeCap, sideHeadroom), Math.min(marginCap, lotCap));
        double idealNotional = idealQty * entry;
        double cappedQty = idealNotional <= notionalCap ? idealQty : notionalCap / entry;
        String sizingNote = describeBinding(idealNotional, perTradeCap, sideHeadroom, marginCap, lotCap, request.side());
        if (volMultiplier < 1.0) {
            // Recorded so a smaller-than-usual position is explainable from its log line alone.
            sizingNote += String.format("; vol overlay x%.3f (realized vol above the %.1f%% daily target)"
                            + " cut the risk fraction to %.3f%%",
                    volMultiplier, config.targetDailyVolFraction() * 100, effectiveRiskFraction * 100);
        }

        // 9 ─ Lot alignment, always downwards, then the exchange's minimums.
        BigDecimal quantity = filters.quantizeQuantityDown(cappedQty);
        if (quantity.signum() <= 0 || !filters.isQuantityInRange(quantity, true)) {
            BigDecimal smallest = filters.smallestTradableQuantity(entry);
            // The effective (overlay-scaled) fraction, so the advice matches the sizing that failed.
            double balanceNeeded = smallest.doubleValue() * Math.abs(entry - stopPrice)
                    / effectiveRiskFraction;
            return RiskDecision.reject(RejectReason.BELOW_MIN_QUANTITY,
                    String.format("sized to %s, below the exchange minimum %s. At this stop distance "
                                    + "(%.4f%% of price) a balance of about $%.2f is needed before %.2f%% "
                                    + "risk buys one tradable lot.",
                            quantity.toPlainString(), smallest.toPlainString(),
                            100.0 * Math.abs(entry - stopPrice) / entry, balanceNeeded,
                            effectiveRiskFraction * 100));
        }
        if (!filters.meetsMinNotional(entryTick, quantity)) {
            return RiskDecision.reject(RejectReason.BELOW_MIN_NOTIONAL,
                    String.format("notional $%.4f is below the exchange minimum $%s",
                            quantity.doubleValue() * entry, filters.minNotional().toPlainString()));
        }

        // 10 ─ Everything from here is computed from the FINAL quantity, never the intended one.
        double qty = quantity.doubleValue();
        double notionalUsd = qty * entry;
        double riskUsd = PositionSizer.riskUsd(qty, entry, stopPrice);
        double riskFraction = riskUsd / balanceUsd;

        if (riskUsd > budgetedRiskUsd * (1 + 1e-9)) {
            // Unreachable while steps 7-9 only shrink; the last place to catch it before an order.
            return RiskDecision.reject(RejectReason.RISK_BUDGET_OVERRUN,
                    String.format("final size risks $%.6f against a budget of $%.6f", riskUsd, budgetedRiskUsd));
        }

        int bracketMaxLeverage = request.marginTiers().maxLeverageAt(notionalUsd);
        if (request.leverage() > bracketMaxLeverage) {
            return RiskDecision.reject(RejectReason.LEVERAGE_ABOVE_EXCHANGE_BRACKET,
                    String.format("%dx requested, but the exchange allows at most %dx at a notional of $%.2f",
                            request.leverage(), bracketMaxLeverage, notionalUsd));
        }

        double initialMarginUsd = notionalUsd / request.leverage();
        if (initialMarginUsd > balanceUsd * config.maxMarginUtilizationFraction() * (1 + 1e-9)) {
            return RiskDecision.reject(RejectReason.INSUFFICIENT_MARGIN,
                    String.format("initial margin $%.2f exceeds the %.0f%% of $%.2f this config will lock up",
                            initialMarginUsd, config.maxMarginUtilizationFraction() * 100, balanceUsd));
        }

        // 11 ─ The liquidation buffer. The one check that can refuse a trade whose sizing is perfect.
        LiquidationSafety.Buffer buffer;
        try {
            buffer = LiquidationSafety.evaluateForPosition(request.side(), entry, stopPrice, qty,
                    request.leverage(), request.marginTiers(), config.takerFeeFraction());
        } catch (IllegalArgumentException e) {
            return RiskDecision.reject(RejectReason.LIQUIDATION_BUFFER,
                    "liquidation price could not be established: " + e.getMessage());
        }
        if (!buffer.satisfies(config.minLiquidationBufferFraction())) {
            int safeLeverage = LiquidationSafety.highestSafeLeverage(request.side(), entry, stopPrice, qty,
                    config.maxLeverage(), request.marginTiers(), config.takerFeeFraction(),
                    config.minLiquidationBufferFraction());
            String remedy = safeLeverage > 0
                    ? "the same trade passes at " + safeLeverage + "x or lower"
                    : "no leverage from 1x to " + config.maxLeverage() + "x passes; the stop is too wide "
                            + "for this instrument's maintenance margin";
            return RiskDecision.reject(RejectReason.LIQUIDATION_BUFFER,
                    String.format("stop %s leaves %.1f%% of the entry-to-liquidation distance (liq %.8g, "
                                    + "minimum %.0f%%) at %dx — %s",
                            stopTick.toPlainString(), buffer.fraction() * 100, buffer.liquidationPrice(),
                            config.minLiquidationBufferFraction() * 100, request.leverage(), remedy));
        }

        // 12 ─ Reduce-only exits, in R.
        List<TakeProfitPolicy.ProjectedLeg> takeProfits =
                config.takeProfitPolicy().project(request.side(), entry, stopPrice, quantity, filters);
        if (takeProfits.isEmpty()) {
            sizingNote += "; no take-profit leg is large enough to be sendable — the stop is the only exit";
        }

        TradePlan plan = new TradePlan(request.signalId(), request.symbol(), request.side(),
                entryTick, stopTick, stop, quantity, request.leverage(), notionalUsd, initialMarginUsd,
                riskUsd, riskFraction, buffer.liquidationPrice(), buffer.fraction(), takeProfits,
                filters, sizingNote);

        LOG.info("[RiskEngine] approved " + plan);
        return RiskDecision.approve(plan);
    }

    /**
     * Takes the <b>filled</b> quantity and average fill price, never the plan's intended numbers:
     * a partial fill is a different position from the approved one.
     *
     * @param protectiveStopId client order id of the stop already resting on the exchange. Required,
     *                         not optional: a filled position is only ever booked here after its stop
     *                         is placed, and reconciliation later asks the exchange about it by name.
     */
    public void registerFill(TradePlan plan, BigDecimal filledQuantity, double averageFillPrice,
                             String protectiveStopId) {
        Preconditions.notNull(plan, "plan");
        Preconditions.notNull(filledQuantity, "filledQuantity");
        Preconditions.require(filledQuantity.signum() > 0, "filledQuantity must be positive");
        Preconditions.positiveFinite(averageFillPrice, "averageFillPrice");
        Preconditions.notBlank(protectiveStopId, "protectiveStopId");

        double qty = filledQuantity.doubleValue();
        book.open(new ExposureBook.OpenPosition(
                plan.symbol(), plan.side(), filledQuantity, averageFillPrice,
                qty * averageFillPrice,
                PositionSizer.riskUsd(qty, averageFillPrice, plan.stopPrice().doubleValue()),
                Optional.of(protectiveStopId)));
    }

    /**
     * Books no realised PnL: that comes from the exchange income ledger via
     * {@link DailyLossKillSwitch#seedRealizedPnl}, and doing both would count it twice.
     */
    public void registerClose(String symbol) {
        book.close(symbol);
    }

    /**
     * A throwing volatility source is treated exactly like one that answered "I do not know". The
     * overlay is advisory: it may only ever shrink an already-budgeted size, so a broken feed must
     * degrade the bot to its pre-overlay behaviour — it must never grow into a new reason to refuse
     * a trade or crash the gate. A null return is the same contract violation as a throw and gets
     * the same treatment; without this line it would surface later as an NPE inside the gate.
     */
    private OptionalDouble realizedVolOrEmpty(String symbol) {
        try {
            OptionalDouble vol = volSource.realizedDailyVolFraction(symbol);
            return vol != null ? vol : OptionalDouble.empty();
        } catch (RuntimeException e) {
            LOG.warning("[RiskEngine] volatility source failed for " + symbol
                    + " — overlay disabled for this decision: " + e);
            return OptionalDouble.empty();
        }
    }

    private static String describeBinding(double idealNotional,
                                          double perTradeCap,
                                          double sideHeadroom,
                                          double marginCap,
                                          double lotCap,
                                          Side side) {
        double tightest = Math.min(Math.min(perTradeCap, sideHeadroom), Math.min(marginCap, lotCap));
        if (idealNotional <= tightest) return "sized by the stop, no ceiling bound";
        if (tightest == perTradeCap) return "reduced by the per-trade notional cap";
        if (tightest == sideHeadroom) return "reduced by remaining " + side + " exposure headroom";
        if (tightest == marginCap) return "reduced by the margin utilisation cap";
        return "reduced by the exchange lot ceiling";
    }
}
