package com.bot.exec;

import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Naming what closed a position, and saying it in the right voice.
 *
 * <p>08.09: VVVUSDT's take-profit filled at 23.368 for +$1.31 - the first take this account ever
 * scored - and the owner was told "WARNING - Exchange-side exit - GHOST_POSITION VVVUSDT: ... it
 * closed without this process noticing". Binance retires a {@code closePosition} conditional order
 * the instant the position goes, so the stop was neither FILLED nor working and the generic branch
 * won. The order history knew all along: the take appears there under "bt-t0-...".
 */
class ExchangeExitCauseTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();
    private final Reconciler reconciler = new Reconciler(exchange, engine, halt, alerts,
            new IdempotentOrderPlacer(exchange, 1, 1, 0, ExecFixtures.NO_SLEEP));

    /** A normally executed long: entry filled, stop resting, take-profit legs resting. */
    private ExecutionCoordinator.Report openOne() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        return ExecFixtures.coordinator(exchange, engine, halt, alerts).execute(plan);
    }

    private static String detailOf(Reconciler.Report report) {
        return report.drifts().stream()
                .filter(d -> d.kind() == Reconciler.Drift.Kind.GHOST_POSITION)
                .map(Reconciler.Drift::detail)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no ghost drift: " + report.describe()));
    }

    @Test
    @DisplayName("a take-profit that filled is named as one, and announced calmly")
    void filledTakeProfitIsNamedAndCalm() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String takeId = opened.takeProfitOrders().get(0).clientOrderId();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();

        // The exchange as it really was: the take filled, the stop was retired with the position,
        // and the position is gone. Asking the stop by name answers EXPIRED, which explains nothing.
        exchange.fillOrder(takeId, "0.041", "23.368");
        exchange.setOrderState(stopId, OrderState.EXPIRED);
        exchange.clearPosition("BTCUSDT");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        String detail = detailOf(report);
        assertTrue(detail.contains("take-profit"), detail);
        assertTrue(detail.contains(takeId), detail);
        assertFalse(detail.contains("without this process noticing"),
                "the cause IS known here: " + detail);
        assertTrue(alerts.sawInfo("Position closed"), alerts.messages.toString());
        assertFalse(alerts.sawWarning("Exchange-side exit"),
                "a winner must not be announced as a fault: " + alerts.messages);
        assertFalse(halt.isHalted());
        assertFalse(engine.book().hasPosition("BTCUSDT"));
    }

    @Test
    @DisplayName("a close the owner made in the app is named as one, and announced calmly")
    void handCloseIsNamedAndCalm() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        exchange.setOrderState(opened.protectiveStop().orElseThrow().clientOrderId(), OrderState.EXPIRED);
        // The Binance app sends a reduce-only market order under its own id.
        exchange.recordFilledOrder("web_88112", "BTCUSDT", OrderType.MARKET, "0.041", "64500.0", true);
        exchange.clearPosition("BTCUSDT");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        String detail = detailOf(report);
        assertTrue(detail.contains("closed by hand"), detail);
        assertTrue(alerts.sawInfo("Position closed"), alerts.messages.toString());
        assertFalse(alerts.sawWarning("Exchange-side exit"),
                "the owner closing his own trade is not a fault: " + alerts.messages);
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("a stop that filled keeps its name and its calm")
    void filledStopIsNamedAndCalm() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        exchange.fillOrder(stopId, "0.041", "62800.0");
        exchange.clearPosition("BTCUSDT");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(detailOf(report).contains("protective stop " + stopId + " filled"), detailOf(report));
        assertTrue(alerts.sawInfo("Position closed"), alerts.messages.toString());
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("a liquidation is never dressed up as a normal exit")
    void liquidationStaysAWarning() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        exchange.setOrderState(opened.protectiveStop().orElseThrow().clientOrderId(), OrderState.EXPIRED);
        // Binance mints this id itself when it liquidates a position.
        exchange.recordFilledOrder("autoclose-1757260000000", "BTCUSDT", OrderType.LIMIT,
                "0.041", "61900.0", false);
        exchange.clearPosition("BTCUSDT");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        String detail = detailOf(report);
        assertTrue(detail.contains("liquidation"), detail);
        assertTrue(alerts.sawWarning("Exchange-side exit"),
                "the worst day on the account must not read like the best: " + alerts.messages);
        assertFalse(alerts.sawInfo("Position closed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("an exit nothing explains is still a warning, and still says so")
    void unexplainedExitStaysAWarning() throws Exception {
        openOne();
        // No order history from this venue and no stop worth asking about: nothing is known.
        exchange.answersOrderHistory = false;
        exchange.hideNextQueries = 5;
        exchange.clearPosition("BTCUSDT");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(detailOf(report).contains("without this process noticing"), detailOf(report));
        assertTrue(alerts.sawWarning("Exchange-side exit"), alerts.messages.toString());
        assertFalse(alerts.sawInfo("Position closed"), alerts.messages.toString());
        assertFalse(halt.isHalted(), "an unexplained exit is reported, never halted on");
    }

    @Test
    @DisplayName("a closing fill from an earlier round trip on the same symbol is not borrowed")
    void staleClosingFillIsNotBorrowed() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String takeId = opened.takeProfitOrders().get(0).clientOrderId();
        // A take that filled an hour BEFORE this position's own entry: a previous trade's exit.
        exchange.fillOrder(takeId, "0.041", "70000.0");
        exchange.ageOrder(takeId, 3_600_000L);
        exchange.setOrderState(opened.protectiveStop().orElseThrow().clientOrderId(), OrderState.EXPIRED);
        exchange.clearPosition("BTCUSDT");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        String detail = detailOf(report);
        assertFalse(detail.contains(takeId),
                "an exit older than this position's own entry proves nothing: " + detail);
        assertTrue(detail.contains("without this process noticing"), detail);
        assertTrue(alerts.sawWarning("Exchange-side exit"), alerts.messages.toString());
    }

    @Test
    @DisplayName("a guessed exit is NOT calm: a resting stop is not proof the take fired")
    void closedEarlyStaysAWarning() throws Exception {
        openOne();
        // The history does not answer, and the stop merely still rests. That is precisely what a
        // liquidation looks like before Binance retires its legs, so "probably a take-profit" may
        // not be announced as the machine working.
        exchange.answersOrderHistory = false;
        exchange.clearPosition("BTCUSDT");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        String detail = detailOf(report);
        assertTrue(detail.contains("still-resting stop"), detail);
        assertTrue(alerts.sawWarning("Exchange-side exit"),
                "a guess must not read as an explanation: " + alerts.messages);
        assertFalse(alerts.sawInfo("Position closed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("an unproven exit does not strip the position of its legs on the spot")
    void unprovenExitKeepsItsLegs() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        // One positionRisk read that did not list the symbol is not proof the position is gone.
        // Cancelling its stop on that alone would strip a live position on a bad read.
        exchange.answersOrderHistory = false;
        exchange.clearPosition("BTCUSDT");

        reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(exchange.order(stopId).orElseThrow().isWorking(),
                "an unexplained disappearance must leave the stop to the grace window and the sweep");
    }

    @Test
    @DisplayName("a pass that only swept orphans is not dressed up as a position closing well")
    void orphanOnlySweepStillWarns() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        // An exit nothing explains: the legs survive the first pass by design, and the book row
        // does not, so from here on the symbol is flat, unbooked and still carrying our orders.
        exchange.answersOrderHistory = false;
        exchange.clearPosition("BTCUSDT");
        reconciler.reconcile(ExecFixtures.NOON);
        assertTrue(exchange.order(stopId).orElseThrow().isWorking());
        exchange.ageOrder(stopId, 600_000L);
        exchange.ageOrder(opened.takeProfitOrders().get(0).clientOrderId(), 600_000L);
        alerts.messages.clear();

        // Whatever pass finally reaches them raises ORPHAN_ORDER and nothing else. allMatch over
        // an empty stream is vacuously true, which turned that pass into a calm line - and this
        // sweep is the alarm that once surfaced 64 dead conditional orders against a ~33 cap.
        for (int pass = 0; pass < 25 && exchange.order(stopId).orElseThrow().isWorking(); pass++) {
            reconciler.reconcile(ExecFixtures.NOON);
        }

        assertFalse(exchange.order(stopId).orElseThrow().isWorking(), "the sweep must still reach it");
        assertFalse(alerts.sawInfo("Leftover orders cancelled"),
                "tidying up after an exit nobody explained is not good news: " + alerts.messages);
    }

    @Test
    @DisplayName("a foreign stop filling is named a stop, not a close the owner just made by hand")
    void foreignStopFillIsNotAHandClose() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        exchange.setOrderState(opened.protectiveStop().orElseThrow().clientOrderId(), OrderState.EXPIRED);
        // A stop the owner placed in the app himself, firing at a loss. Calling that "closed by
        // hand" tells him he did something he did not do, on a trade that went against him.
        exchange.recordFilledOrder("web_stop_7", "BTCUSDT", OrderType.STOP_MARKET,
                "0.041", "61000.0", true);
        exchange.clearPosition("BTCUSDT");

        String detail = detailOf(reconciler.reconcile(ExecFixtures.NOON));

        assertTrue(detail.contains("stop"), detail);
        assertFalse(detail.contains("by hand"), "it fired on its own, he did not press anything: " + detail);
    }

    @Test
    @DisplayName("a venue auto-close is a liquidation whatever punctuation its id carries")
    void autoCloseVariantIsStillALiquidation() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        exchange.setOrderState(opened.protectiveStop().orElseThrow().clientOrderId(), OrderState.EXPIRED);
        // One character off the prefix that was matched exactly, and the worst day on the account
        // used to be announced calmly as a close made by hand.
        exchange.recordFilledOrder("AUTOCLOSE_1757260000000", "BTCUSDT", OrderType.LIMIT,
                "0.041", "61900.0", false);
        exchange.clearPosition("BTCUSDT");

        String detail = detailOf(reconciler.reconcile(ExecFixtures.NOON));

        assertTrue(detail.contains("liquidation"), detail);
        assertTrue(alerts.sawWarning("Exchange-side exit"), alerts.messages.toString());
        assertFalse(alerts.sawInfo("Position closed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("the protective legs of a position that just closed are cancelled on that same pass")
    void leftoverLegsGoOnTheSamePass() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        String takeId = opened.takeProfitOrders().get(0).clientOrderId();
        // Closed by hand seconds ago: both legs are younger than the 60s orphan grace, and both
        // are now guarding nothing. Waiting for the grace, or for the ~10-minute account sweep,
        // leaves live conditional orders on a flat symbol.
        exchange.recordFilledOrder("web_99001", "BTCUSDT", OrderType.MARKET, "0.041", "64500.0", true);
        exchange.clearPosition("BTCUSDT");
        assertTrue(exchange.order(stopId).orElseThrow().isWorking());

        reconciler.reconcile(ExecFixtures.NOON);

        assertFalse(exchange.order(stopId).orElseThrow().isWorking(),
                "the stop of a closed position is an orphan immediately, not after a grace window");
        assertFalse(exchange.order(takeId).orElseThrow().isWorking(),
                "so is the take-profit leg");
    }

    @Test
    @DisplayName("where the venue lists no conditional orders, the stop still goes by the name on record")
    void leftoverStopGoesByNameOnANonListingVenue() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        exchange.recordFilledOrder("web_99002", "BTCUSDT", OrderType.MARKET, "0.041", "64500.0", true);
        exchange.clearPosition("BTCUSDT");
        // demo-fapi: the stop rests and answers by id, but no listing can see it - so neither the
        // flat branch nor the account-wide sweep would ever reach it.
        exchange.listsConditionalOrders = false;

        reconciler.reconcile(ExecFixtures.NOON);

        assertFalse(exchange.order(stopId).orElseThrow().isWorking(),
                "a stop nothing can list must still be cancelled by the id the book kept");
    }

    @Test
    @DisplayName("the journal hears the proven cause, not just the prose")
    void journalHearsTheCause() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String takeId = opened.takeProfitOrders().get(0).clientOrderId();
        exchange.fillOrder(takeId, "0.041", "23.368");
        exchange.setOrderState(opened.protectiveStop().orElseThrow().clientOrderId(), OrderState.EXPIRED);
        exchange.clearPosition("BTCUSDT");

        List<String> rows = new ArrayList<>();
        reconciler.onExchangeExit((symbol, cause, detail, orderId, price, qty) ->
                rows.add(symbol + "|" + cause + "|" + orderId + "|" + price + "|" + qty));

        reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(List.of("BTCUSDT|take-profit|" + takeId + "|23.368|0.041"), rows,
                "a later analysis must be able to tell a take-profit from a hand close");
    }

    @Test
    @DisplayName("a hand close reaches the journal under its own cause")
    void journalHearsAHandClose() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        exchange.setOrderState(opened.protectiveStop().orElseThrow().clientOrderId(), OrderState.EXPIRED);
        exchange.recordFilledOrder("web_77", "BTCUSDT", OrderType.MARKET, "0.041", "64500.0", true);
        exchange.clearPosition("BTCUSDT");

        List<String> causes = new ArrayList<>();
        reconciler.onExchangeExit((symbol, cause, detail, orderId, price, qty) -> causes.add(cause));

        reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(List.of("hand-close"), causes);
    }

    @Test
    @DisplayName("the happy path asks the order history nothing")
    void noHistoryCallWhenNothingClosed() throws Exception {
        CountingHistory counting = new CountingHistory();
        TradePlan plan = ExecFixtures.approvedPlan(engine, counting.delegate.fetchFilters("BTCUSDT"));
        ExecFixtures.coordinator(counting.delegate, engine, halt, alerts).execute(plan);
        Reconciler viaCounting = new Reconciler(counting, engine, halt, alerts,
                new IdempotentOrderPlacer(counting, 1, 1, 0, ExecFixtures.NO_SLEEP));

        Reconciler.Report report = viaCounting.reconcile(ExecFixtures.NOON);

        assertTrue(report.converged(), report.describe());
        assertEquals(0, counting.calls,
                "the loop runs every 30s against a rate limit; the history is for the rare flat pass");
    }

    /** Counts the extra listing, to keep it off the happy path. */
    private static final class CountingHistory extends DelegatingExchange {
        int calls = 0;

        @Override public List<OrderStatus> recentOrders(String symbol, long sinceEpochMs) {
            calls++;
            return super.recentOrders(symbol, sinceEpochMs);
        }
    }
}
