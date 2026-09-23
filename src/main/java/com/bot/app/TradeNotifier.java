package com.bot.app;

import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;

import static com.bot.app.TgFormat.balance;
import static com.bot.app.TgFormat.esc;
import static com.bot.app.TgFormat.held;
import static com.bot.app.TgFormat.money;
import static com.bot.app.TgFormat.pct;
import static com.bot.app.TgFormat.plural;
import static com.bot.app.TgFormat.price;
import static com.bot.app.TgFormat.shortSymbol;
import static com.bot.app.TgFormat.time;

/**
 * One Telegram line per trade: every entry fill and every exit, in the owner's language. Until
 * 23.09 an entry was silent and an exit arrived as English prose from the reconciler ("Position
 * closed - GHOST_POSITION ... its take-profit bt-t0-... filled at 23.368"), so "what did the bot do
 * today" meant opening /book twice and subtracting.
 *
 * <p>Called on the MAIN LOOP, next to the journal and never instead of it, so nothing here may
 * block or throw: messages go to a non-blocking outbox (a separate thread sends them), and every
 * public method swallows its own failures. It keeps no trading state - only what it needs to say
 * how a position ended (its side, entry, size and open time), fed by the entries it announced and
 * by the snapshot the loop already builds for /book.
 *
 * <p>{@code QUIET_HOURS} (Warsaw) holds trade lines in memory and sends one 🌙 summary when the
 * window ends. The buffer is memory only: a restart inside the window loses it (the journal and
 * /pnl still have everything). Alerts never pass through here, so a warning is never held.
 */
final class TradeNotifier {

    private static final Logger LOG = Logger.getLogger(TradeNotifier.class.getName());

    /** A night of a full book is ~30 lines; past this only the count is kept. */
    static final int MAX_HELD = 200;
    /** Lines shown in the summary; the rest is a count, so the summary stays one message. */
    static final int SUMMARY_LINES = 40;

    /** How a position ended, as the owner reads it. The emoji is the chat's vocabulary. */
    enum ExitKind {
        TAKE("✅", "Тейк", ""),
        STOP("🛑", "Стоп", ""),
        TIME("⏱", "По времени", "срок удержания вышел"),
        TREND("⏱", "Тренд кончился", "сканер закрыл по правилу выхода"),
        MANUAL("✋", "Закрыта вручную", "командой /close"),
        CLOSE_ALL("🧯", "Закрыта", "команда «закрыть всё»"),
        HAND("✋", "Закрыта в приложении", ""),
        BOT("✋", "Закрыта ботом", ""),
        KILL_SWITCH("⏸", "Дневной лимит", "книга закрывается по дневному лимиту"),
        STOP_REPAIR("🔴", "Закрыта без стопа", "стоп не нашёлся — закрыта reduce-only"),
        CLOSED_EARLY("🟠", "Закрыта до стопа", "тейк или вручную — биржа не уточнила"),
        LIQUIDATION("🔴", "Ликвидация", "биржа закрыла сама — проверь счёт"),
        UNEXPLAINED("🟠", "Позиция ушла", "причину биржа не назвала"),
        OTHER("✋", "Закрыта", "");

        final String emoji;
        final String title;
        final String note;

        ExitKind(String emoji, String title, String note) {
            this.emoji = emoji;
            this.title = title;
            this.note = note;
        }

        /** The reconciler's {@code ExitCause} token, as the journal row carries it. */
        static ExitKind ofCause(String token) {
            return switch (token == null ? "" : token) {
                case "take-profit" -> TAKE;
                case "stop-loss" -> STOP;
                case "hand-close" -> HAND;
                case "bot-close" -> BOT;
                case "closed-early" -> CLOSED_EARLY;
                case "liquidation" -> LIQUIDATION;
                default -> UNEXPLAINED;
            };
        }

        /**
         * A close this process commanded, by the reason its request carries: the operator's two
         * Telegram paths, the scanner's rules ("max-hold", "trend-exited", "dip-exited", ...),
         * the kill switch and the stop repair. Anything else is shown with its own words.
         */
        static ExitKind ofCloseReason(String reason) {
            String r = reason == null ? "" : reason.trim();
            if (r.equals("operator via Telegram")) return MANUAL;
            if (r.equals(OperatorChannel.CLOSE_ALL_REASON)) return CLOSE_ALL;
            if (r.equals("max-hold")) return TIME;
            if (r.endsWith("-exited")) return TREND;
            if (r.equals("daily-loss-kill-switch")) return KILL_SWITCH;
            if (r.equals("stop-repair")) return STOP_REPAIR;
            return OTHER;
        }
    }

    /**
     * {@code QUIET_HOURS} as a Warsaw wall-clock window, {@code 23-08} or {@code 23:30-07:15}.
     * Wall clock on purpose: "23 to 8" means the owner's night on both sides of a DST change.
     */
    record QuietHours(LocalTime start, LocalTime end) {

        private static final DateTimeFormatter HH_MM = DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT);

        QuietHours {
            Preconditions.notNull(start, "start");
            Preconditions.notNull(end, "end");
            Preconditions.require(!start.equals(end), "an empty quiet window");
        }

        /** Null for blank or "off"; throws on anything else that is not a window. */
        static QuietHours parse(String raw) {
            String s = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
            if (s.isEmpty() || s.equals("off") || s.equals("none")) return null;
            String[] ends = s.split("\\s*[-–]\\s*");
            if (ends.length != 2) throw new IllegalArgumentException("QUIET_HOURS must look like 23-08");
            return new QuietHours(clock(ends[0]), clock(ends[1]));
        }

        private static LocalTime clock(String s) {
            String[] hm = s.split(":");
            if (hm.length > 2) throw new IllegalArgumentException("not a time: " + s);
            int h = Integer.parseInt(hm[0].trim());
            int m = hm.length == 2 ? Integer.parseInt(hm[1].trim()) : 0;
            if (h == 24 && m == 0) h = 0;
            return LocalTime.of(h, m);
        }

        boolean contains(Instant at) {
            LocalTime t = at.atZone(TgFormat.WARSAW).toLocalTime();
            if (start.isBefore(end)) return !t.isBefore(start) && t.isBefore(end);
            return !t.isBefore(start) || t.isBefore(end);
        }

        boolean overnight() {
            return end.isBefore(start);
        }

        String label() {
            return HH_MM.format(start) + "–" + HH_MM.format(end);
        }
    }

    /** What an exit needs to be told well: the position's side, entry, size, open time and plan. */
    private record Open(Side side, double entry, double qty, Instant openedAt, double stop, double take) {}

    /** One held line of the night: when, the line, its P&L (NaN when none or unknown). */
    private record HeldLine(Instant at, String line, double pnl, boolean entry, boolean exit, boolean approximate) {}

    private final Consumer<String> out;
    private final QuietHours quiet;
    private final Map<String, Open> open = new HashMap<>();
    private final List<HeldLine> held = new ArrayList<>();
    private int overflow;

    TradeNotifier(Consumer<String> out, QuietHours quiet) {
        this.out = Preconditions.notNull(out, "out");
        this.quiet = quiet;
    }

    /**
     * From {@code TRADE_NOTIFY} (on by default; off/0/false/no turns it off) and
     * {@code QUIET_HOURS}. Null when notifications are off. A window that does not parse is
     * logged and ignored - a typo must cost the quiet, not the notifications.
     */
    static TradeNotifier fromEnvironmentOrNull(UnaryOperator<String> env, Consumer<String> out) {
        String flag = env.apply("TRADE_NOTIFY");
        String f = flag == null ? "" : flag.trim().toLowerCase(Locale.ROOT);
        if (f.equals("off") || f.equals("0") || f.equals("false") || f.equals("no")) {
            LOG.info("[Notify] TRADE_NOTIFY=off - no per-trade Telegram lines");
            return null;
        }
        if (!f.isEmpty() && !f.equals("on") && !f.equals("1") && !f.equals("true") && !f.equals("yes")) {
            LOG.warning("[Notify] TRADE_NOTIFY=\"" + flag + "\" is neither on nor off - leaving it on");
        }
        QuietHours quiet = null;
        try {
            quiet = QuietHours.parse(env.apply("QUIET_HOURS"));
        } catch (RuntimeException e) {
            LOG.warning("[Notify] QUIET_HOURS=\"" + env.apply("QUIET_HOURS")
                    + "\" is not a window like 23-08 - quiet hours are OFF: " + e.getMessage());
        }
        if (quiet != null) {
            LOG.info("[Notify] quiet hours " + quiet.label() + " Warsaw: trade lines held, one summary after");
        }
        return new TradeNotifier(out, quiet);
    }

    QuietHours quietHours() {
        return quiet;
    }

    // ─── Events (main loop) ──────────────────────────────────────────────────────────────────

    /** An entry filled. Remembered, so its exit can say how long it was held and what it made. */
    synchronized void entry(String symbol, Side side, double fill, double qty, double stop, double take,
                            double riskUsd, Instant at) {
        try {
            open.put(symbol, new Open(side, fill, qty, at, stop, take));
            send(entryHtml(symbol, side, fill, stop, take, riskUsd),
                    new HeldLine(at, "🟢 Вход " + side.name() + " " + esc(shortSymbol(symbol)), Double.NaN,
                            true, false, false), at);
        } catch (RuntimeException e) {
            LOG.warning("[Notify] entry line for " + symbol + " not built: " + e.getMessage());
        }
    }

    /**
     * A position left the book. {@code exitPrice}/{@code exitQty} are the closing order's when
     * the path had them (NaN otherwise); the P&amp;L is computed from the entry and marked "≈" -
     * fees and funding are not in it, /pnl has the exchange's own net figure.
     *
     * @param note an extra line for {@link ExitKind#OTHER} (the close reason) or a qualifier like
     *             "пока бот стоял"; blank for none
     */
    synchronized void exit(String symbol, ExitKind kind, double exitPrice, double exitQty, Instant at, String note) {
        try {
            Open o = open.remove(symbol);
            double px = exitPrice > 0 ? exitPrice : Double.NaN;
            if (Double.isNaN(px) && o != null) {
                // The order that closed it was not readable; the level it rested at is the honest guess.
                if (kind == ExitKind.STOP && o.stop() > 0) px = o.stop();
                if (kind == ExitKind.TAKE && o.take() > 0) px = o.take();
            }
            double qty = exitQty > 0 ? exitQty : o == null ? Double.NaN : o.qty();
            double pnl = Double.NaN, move = Double.NaN;
            if (o != null && o.entry() > 0 && px > 0) {
                move = (px / o.entry() - 1.0) * 100.0 * o.side().sign();
                if (qty > 0) pnl = (px - o.entry()) * qty * o.side().sign();
            }
            String head = kind.emoji + " " + kind.title + " " + esc(shortSymbol(symbol));
            StringBuilder sb = new StringBuilder(kind.emoji).append(" <b>").append(kind.title).append(' ')
                    .append(esc(shortSymbol(symbol)));
            if (Double.isFinite(pnl)) sb.append(" ≈").append(money(pnl));
            sb.append("</b>");
            if (Double.isFinite(move)) sb.append(" (").append(pct(move)).append(')');
            List<String> facts = new ArrayList<>();
            if (o != null && o.side() == Side.SHORT) facts.add("шорт");
            if (o != null && o.openedAt() != null && at != null) {
                facts.add("держал " + held(Duration.between(o.openedAt(), at)));
            }
            if (o != null && o.entry() > 0 && px > 0) facts.add(price(o.entry()) + " → " + price(px));
            else if (px > 0) facts.add("выход " + price(px));
            else facts.add("цена выхода неизвестна");
            sb.append('\n').append(String.join(" · ", facts));
            String extra = note == null || note.isBlank() ? kind.note : note.trim();
            if (kind == ExitKind.OTHER && note != null && !note.isBlank()) extra = "причина: " + note.trim();
            if (!extra.isEmpty()) sb.append("\n<i>").append(esc(extra)).append("</i>");
            String line = head + (Double.isFinite(pnl) ? " ≈" + money(pnl) : "");
            send(sb.toString(), new HeldLine(at, line, pnl, false, true, true), at);
        } catch (RuntimeException e) {
            LOG.warning("[Notify] exit line for " + symbol + " not built: " + e.getMessage());
        }
    }

    /** The position shrank: still open, still under its stop, and the leg behind it is unnamed. */
    synchronized void partial(String symbol, Instant at) {
        try {
            String sym = esc(shortSymbol(symbol));
            send("🟠 <b>Частичный выход " + sym + "</b>\nпозиция уменьшилась, остаток под стопом",
                    new HeldLine(at, "🟠 Частичный выход " + sym, Double.NaN, false, false, false), at);
        } catch (RuntimeException e) {
            LOG.warning("[Notify] partial line for " + symbol + " not built: " + e.getMessage());
        }
    }

    /**
     * The loop's view of the book after a reconcile pass. Upserts only: an exit is what removes a
     * position here, so a snapshot built from a stale read cannot forget an entry just announced.
     */
    synchronized void observe(List<OperatorSnapshot.Position> positions) {
        try {
            if (positions == null) return;
            for (OperatorSnapshot.Position p : positions) {
                Open before = open.get(p.symbol());
                boolean same = before != null && before.side() == p.side();
                Instant openedAt = p.openedAt() != null ? p.openedAt() : same ? before.openedAt() : null;
                double take = p.take() > 0 ? p.take() : same ? before.take() : Double.NaN;
                double stop = p.stop() > 0 ? p.stop() : same ? before.stop() : Double.NaN;
                double entry = p.entry() > 0 ? p.entry() : same ? before.entry() : Double.NaN;
                open.put(p.symbol(), new Open(p.side(), entry, p.quantity(), openedAt, stop, take));
            }
        } catch (RuntimeException e) {
            LOG.warning("[Notify] book view not absorbed: " + e.getMessage());
        }
    }

    /** Sends the night's summary once the quiet window is over. Cheap; called every ~30 s. */
    synchronized void tick(Instant now) {
        try {
            if (!held.isEmpty() && (quiet == null || !quiet.contains(now))) flushHeld();
        } catch (RuntimeException e) {
            LOG.warning("[Notify] summary not built: " + e.getMessage());
        }
    }

    /** For a test: lines held for the summary. */
    synchronized int heldCount() {
        return held.size() + overflow;
    }

    // ─── Delivery ────────────────────────────────────────────────────────────────────────────

    private void send(String html, HeldLine line, Instant at) {
        if (quiet != null && at != null && quiet.contains(at)) {
            if (held.size() < MAX_HELD) held.add(line);
            else overflow++;
            return;
        }
        // A morning's first trade must not overtake the night it follows.
        if (!held.isEmpty()) flushHeld();
        out.accept(html);
    }

    private void flushHeld() {
        List<HeldLine> lines = new ArrayList<>(held);
        int extra = overflow;
        held.clear();
        overflow = 0;
        out.accept(summaryHtml(lines, extra, quiet));
    }

    // ─── Rendering (pure) ────────────────────────────────────────────────────────────────────

    /** {@code 🟢 Вход LONG TIA} / {@code цена 0.5075 · риск $0.72} / {@code стоп −12.9% · тейк +22.6%}. */
    static String entryHtml(String symbol, Side side, double fill, double stop, double take, double riskUsd) {
        StringBuilder sb = new StringBuilder("🟢 <b>Вход ").append(side.name()).append(' ')
                .append(esc(shortSymbol(symbol))).append("</b>");
        sb.append("\nцена ").append(price(fill));
        if (Double.isFinite(riskUsd) && riskUsd > 0) sb.append(" · риск ").append(balance(riskUsd));
        sb.append("\nстоп ").append(pct(level(stop, fill))).append(" · тейк ").append(pct(level(take, fill)));
        return sb.toString();
    }

    /** Signed move from the fill to a level, percent: a long's stop reads negative. */
    private static double level(double level, double fill) {
        if (!(level > 0) || !(fill > 0)) return Double.NaN;
        return (level / fill - 1.0) * 100.0;
    }

    private static String summaryHtml(List<HeldLine> lines, int overflow, QuietHours quiet) {
        long entries = lines.stream().filter(HeldLine::entry).count();
        long exits = lines.stream().filter(HeldLine::exit).count();
        List<String> counts = new ArrayList<>();
        if (entries > 0) counts.add(entries + " " + plural(entries, "вход", "входа", "входов"));
        if (exits > 0) counts.add(exits + " " + plural(exits, "выход", "выхода", "выходов"));
        if (counts.isEmpty()) counts.add(lines.size() + " " + plural(lines.size(), "событие", "события", "событий"));
        String when = quiet == null || quiet.overnight() ? "За ночь" : "За тихие часы";
        StringBuilder sb = new StringBuilder("🌙 <b>").append(when).append(": ").append(String.join(", ", counts))
                .append("</b>");
        double total = 0;
        boolean anyPnl = false, unknown = false;
        for (HeldLine l : lines) {
            if (!l.exit()) continue;
            if (Double.isFinite(l.pnl())) {
                total += l.pnl();
                anyPnl = true;
            } else {
                unknown = true;
            }
        }
        if (anyPnl) {
            sb.append("\n💰 ≈").append(money(total)).append(" по закрытым");
            if (unknown) sb.append(" (не все с ценой)");
        }
        int shown = Math.min(lines.size(), SUMMARY_LINES);
        for (int i = 0; i < shown; i++) {
            HeldLine l = lines.get(i);
            sb.append('\n').append(time(l.at())).append(' ').append(l.line());
        }
        int rest = lines.size() - shown + overflow;
        if (rest > 0) sb.append("\n… и ещё ").append(rest);
        return sb.append("\n<i>Сейчас: /book · /pnl</i>").toString();
    }
}
