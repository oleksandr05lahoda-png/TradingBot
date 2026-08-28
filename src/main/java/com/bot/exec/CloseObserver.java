package com.bot.exec;

/**
 * Hears a reduce-only close the machine made on its own — the daily-loss flatten and the
 * reconciler's stop repair — so the trade journal records an exit no operator ever typed. Wired to
 * {@code TradeJournal::closed}; never trading logic, and never allowed to throw into the caller.
 */
@FunctionalInterface
public interface CloseObserver {
    void closed(String requestId, String symbol, ExecutionCoordinator.CloseReport report);
}
