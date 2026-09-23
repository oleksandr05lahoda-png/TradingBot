package com.bot.app;

import com.bot.core.Preconditions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static com.bot.app.TgFormat.ago;
import static com.bot.app.TgFormat.age;
import static com.bot.app.TgFormat.balance;
import static com.bot.app.TgFormat.esc;
import static com.bot.app.TgFormat.money;
import static com.bot.app.TgFormat.padLeft;
import static com.bot.app.TgFormat.padRight;
import static com.bot.app.TgFormat.pct;
import static com.bot.app.TgFormat.positions;
import static com.bot.app.TgFormat.shortSymbol;
import static com.bot.app.TgFormat.time;

/**
 * What the operator reads: every screen of the Telegram interface as a pure function of the loop's
 * snapshot, the halt latch and the clock. Russian, phone-width, the first line always the state.
 * The emoji are a vocabulary, not decoration - 🟢 ok, 🔴 problem, 🟠 warning, ⏸ halt, 👁 observe,
 * 📊 status, 📒 book, 💰 P&amp;L, ⏳ queue, 🧯 close all - so a glance at the first character is
 * already an answer.
 */
final class OperatorViews {

    private OperatorViews() {}

    /** One inline button; Telegram refuses callback data over 64 bytes, so it is checked here. */
    record Button(String text, String data) {
        Button {
            Preconditions.notBlank(text, "text");
            Preconditions.notBlank(data, "data");
            Preconditions.require(data.getBytes(StandardCharsets.UTF_8).length <= 64,
                    "callback_data over 64 bytes: " + data);
        }
    }

    /** A screen: HTML text plus an optional inline keyboard (null = none). */
    record Reply(String html, List<List<Button>> keyboard) {
        static Reply of(String html) {
            return new Reply(html, null);
        }
    }

    // Callback data. Short on purpose: the 64-byte ceiling, and a token rides on two of them.
    static final String CB_MENU = "m:menu";
    static final String CB_STATUS = "m:status";
    static final String CB_BOOK = "m:book";
    static final String CB_PNL = "m:pnl";
    static final String CB_QUEUE = "m:queue";
    static final String CB_HALT = "m:halt";
    static final String CB_RESUME = "m:resume";
    static final String CB_CLOSE_ALL = "m:closeall";
    static final String CB_CLOSE_ALL_YES = "ca:y:";
    static final String CB_CLOSE_ALL_NO = "ca:n:";
    static final String CB_RESUME_YES = "rs:y:";
    static final String CB_RESUME_NO = "rs:n:";
    /** {@code w:ADAUSDT} - /why for one coin. Symbols are checked to fit the 64-byte ceiling. */
    static final String CB_WHY = "w:";

    /**
     * Control plus one fallback screen (24.09). The owner reads the book, P&amp;L and queue in the
     * lab bot's 📊 Панель now, and the same numbers in three places was the complaint. 📒 💰 ⏳ and
     * the per-coin /why buttons are gone from here; their callbacks still answer, because old
     * messages in the chat carry them and a dead button that spins is worse than an old screen.
     */
    static final List<List<Button>> MENU = List.of(
            List.of(new Button("📊 Статус", CB_STATUS)),
            List.of(new Button("⏸ Халт", CB_HALT), new Button("▶️ Снять халт", CB_RESUME)),
            List.of(new Button("🧯 Закрыть всё", CB_CLOSE_ALL)));

    /**
     * The last line of /status: where the full picture lives. The panel is sent by the lab bot,
     * which runs on the owner's laptop - so it says when it is there. Broken in two only so every
     * line of /status stays phone-width (the ≤40-character rule the screen is tested against).
     */
    static final String PANEL_HINT = "<i>Полная панель — кнопка 📊 Панель\nв лаб-боте (когда ноутбук включён)</i>";

    /** The loop publishes every 30 s; three minutes of silence means it is parked or stuck. */
    static final Duration STALE_AFTER = Duration.ofMinutes(3);

    /** Width of a label column in the two-column tables; the whole row stays under ~34 characters. */
    private static final int LABEL = 11;

    // ─── The first line ──────────────────────────────────────────────────────────────────────

    /**
     * The one line that answers "is it all right": the worst thing true right now. A red loop or
     * exchange outranks a halt, a halt outranks the daily limit, and only then is it green.
     */
    static String stateLine(OperatorSnapshot s, Optional<String> haltReason, Instant now) {
        return states(s, haltReason, now).get(0);
    }

    /**
     * Every state that is true, worst first; the first is the headline. A stale loop or a silent
     * exchange outranks a halt, a halt outranks the daily limit, and only then is it green.
     */
    static List<String> states(OperatorSnapshot s, Optional<String> haltReason, Instant now) {
        return stateList(s, haltReason, now).stream().map(State::html).toList();
    }

    /**
     * The headline's machine name, for the panel file (bot_view.json): {@code trading}, {@code halt},
     * {@code kill_switch}, {@code observe}, {@code exchange_hold}, {@code loop_silent},
     * {@code contact_lost}, {@code blind} or {@code starting}. Same ranking as the line /status shows,
     * because it is the same list - a second copy of the logic would drift from the screen.
     */
    static String stateCode(OperatorSnapshot s, Optional<String> haltReason, Instant now) {
        return stateList(s, haltReason, now).get(0).code();
    }

    /** One true state: its code for the panel file, and the line /status renders for it. */
    record State(String code, String html) {}

    private static List<State> stateList(OperatorSnapshot s, Optional<String> haltReason, Instant now) {
        if (s == null) return List.of(new State("starting", "🟠 <b>Запуск</b> — цикл ещё не прислал данных"));
        java.util.ArrayList<State> out = new java.util.ArrayList<>();
        Duration silent = Duration.between(s.at(), now);
        // The hold first: it is the usual reason the loop falls silent, and it names when it ends.
        if (s.exchangeHoldMs() > 0) {
            out.add(new State("exchange_hold",
                    "🔴 <b>Биржа не отвечает</b> — пауза ещё " + age(Duration.ofMillis(s.exchangeHoldMs()))));
        }
        if (silent.compareTo(STALE_AFTER) > 0) {
            out.add(new State("loop_silent", "🔴 <b>Цикл молчит " + age(silent) + "</b>"));
        }
        if (s.contactLost()) {
            out.add(new State("contact_lost", "🔴 <b>Биржа не отвечает</b> — нет связи, входы на паузе"));
        }
        if (s.blind()) {
            out.add(new State("blind",
                    "🔴 <b>Биржа не отвечает</b> — " + s.reconcileFailures() + " сверок подряд не прошли"));
        }
        // Observe is the venue's mode, not only a latch: a drift halt can stand in its place, and a
        // /resume leaves the latch empty until the loop's next pass - the screen said 🟢 then while
        // every signal was refused. The published flag is the truth; the latch only words it.
        boolean observeLatched = haltReason.isPresent()
                && haltReason.get().startsWith(OperatorChannel.OBSERVE_REASON_PREFIX);
        if (s.observeOnly() || observeLatched) out.add(new State("observe", "👁 <b>Наблюдение</b> — входы выключены"));
        if (haltReason.isPresent() && !observeLatched) {
            out.add(new State("halt", "⏸ <b>Халт:</b> " + esc(haltReason.get())));
        }
        if (s.killSwitchTripped()) {
            // How far the day fell, when the switch said (24.09): "paused" alone left the owner asking.
            OperatorSnapshot.KillSwitch ks = s.killSwitch();
            String loss = ks != null && Double.isFinite(ks.dayLossFrac()) && ks.dayLossFrac() > 0
                    ? " " + pct(-ks.dayLossFrac() * 100.0) : "";
            out.add(new State("kill_switch", "⏸ <b>Дневной лимит убытка</b>" + loss + " — входов нет"));
        }
        if (out.isEmpty()) out.add(new State("trading", "🟢 <b>Торгует</b>"));
        return out;
    }

    // ─── /status ─────────────────────────────────────────────────────────────────────────────

    static String status(OperatorSnapshot s, Optional<String> haltReason, Instant now) {
        List<String> states = states(s, haltReason, now);
        StringBuilder sb = new StringBuilder(states.get(0));
        if (s == null) {
            return sb.append("\nПервые данные — через полминуты после старта.\n").append(PANEL_HINT).toString();
        }
        // The headline names only the worst state; the others must still be on the screen, or
        // /status says one thing while the bot quietly refuses every signal for another.
        for (int i = 1; i < states.size(); i++) {
            sb.append('\n').append(states.get(i).replace("<b>", "").replace("</b>", ""));
        }
        if (Duration.between(s.at(), now).compareTo(STALE_AFTER) > 0) {
            sb.append("\n<i>Ниже — последние данные цикла.\nСтопы стоят на бирже и работают.</i>");
        }

        OperatorSnapshot.Account a = s.account();
        long longs = s.positions().stream().filter(p -> p.side() == com.bot.core.Side.LONG).count();
        long shorts = s.positions().size() - longs;
        sb.append("\n<pre>");
        row(sb, "Кошелёк", a == null ? "—" : balance(a.wallet()), true);
        row(sb, "Маржа", a == null ? "—" : balance(a.marginInUse()), false);
        row(sb, "Свободно", a == null ? "—" : balance(a.available()), false);
        row(sb, "Нереал.", a == null ? "—" : money(a.unrealized()), false);
        row(sb, "Позиции", s.positions().size() + (s.positions().isEmpty() ? "" : " · L" + longs + " S" + shorts), false);
        row(sb, "Закрытия", s.pendingCloses() == 0 ? "нет" : s.pendingCloses() + " в повторе", false);
        row(sb, "Сверка", ago(s.lastReconcileOkAt(), now), false);
        row(sb, "Работает", age(Duration.between(s.startedAt(), now)), false);
        row(sb, "Сборка", esc(s.buildStamp()), false);
        sb.append("</pre>");
        List<String> noStop = s.positions().stream()
                .filter(p -> s.positionsFromExchange() && !p.stopOnRecord())
                .map(p -> esc(p.symbol())).toList();
        if (!noStop.isEmpty()) sb.append("\n🟠 Без стопа в книге: ").append(String.join(", ", noStop));
        if (s.pendingCloses() > 0) {
            // Named here, not behind /queue: /status is the one screen this bot still advertises.
            List<String> retrying = s.closeQueue().stream().map(c -> esc(shortSymbol(c.symbol()))).distinct().toList();
            sb.append("\n🟠 Закрытия повторяются: ")
                    .append(retrying.isEmpty() ? Integer.toString(s.pendingCloses()) : String.join(", ", retrying));
        }
        return sb.append('\n').append(footer(s.at(), now)).append('\n').append(PANEL_HINT).toString();
    }

    private static void row(StringBuilder sb, String label, String value, boolean first) {
        if (!first) sb.append('\n');
        sb.append(padRight(label, LABEL)).append(value);
    }

    private static String footer(Instant at, Instant now) {
        return "<i>Данные " + ago(at, now) + " · " + time(at) + "</i>";
    }

    // ─── /book ───────────────────────────────────────────────────────────────────────────────

    static String book(OperatorSnapshot s, Instant now) {
        if (s == null) return "🟠 <b>Книги ещё нет</b> — цикл не прислал данных";
        List<OperatorSnapshot.Position> rows = s.positions();
        if (rows.isEmpty()) {
            return "📒 <b>Книга пуста</b>\nОткрытых позиций нет.\n" + footer(s.at(), now);
        }
        double total = 0;
        boolean anyPnl = false;
        for (OperatorSnapshot.Position p : rows) {
            if (Double.isFinite(p.unrealizedUsd())) {
                total += p.unrealizedUsd();
                anyPnl = true;
            }
        }
        StringBuilder sb = new StringBuilder("📒 <b>Книга: ").append(positions(rows.size())).append("</b>");
        sb.append("\n<pre>").append(bookHeader());
        for (OperatorSnapshot.Position p : rows) sb.append('\n').append(esc(bookRow(p, now)));
        sb.append("</pre>");
        sb.append("\n💰 Нереал.: ").append(anyPnl ? money(total) : "—");
        sb.append("\n<i>Стоп/Тейк — сколько пройти цене.</i>");
        if (!s.positionsFromExchange()) sb.append("\n🟠 Цен с биржи ещё нет — показана книга бота");
        return sb.append('\n').append(footer(s.at(), now)).toString();
    }

    /** {@code Монета   ·    P&L   Стоп   Тейк  ч} - 34 columns, the width of a phone in portrait. */
    static String bookHeader() {
        return esc(padRight("Монета", 8) + " · " + padLeft("P&L", 6) + " " + padLeft("Стоп", 6)
                + " " + padLeft("Тейк", 6) + " " + padLeft("ч", 2));
    }

    /** Plain text; the caller escapes. */
    static String bookRow(OperatorSnapshot.Position p, Instant now) {
        long h = p.hoursHeld(now);
        return padRight(shortSymbol(p.symbol()), 8) + " " + (p.side() == com.bot.core.Side.LONG ? "L" : "S") + " "
                + padLeft(pct(p.pnlPct()), 6) + " " + padLeft(pct(p.toStopPct()), 6) + " "
                + padLeft(pct(p.toTakePct()), 6) + " " + padLeft(h < 0 ? "—" : Long.toString(h), 2);
    }

    // ─── /pnl ────────────────────────────────────────────────────────────────────────────────

    static String pnl(OperatorSnapshot s, Instant now) {
        OperatorSnapshot.Pnl p = s == null ? null : s.pnl();
        if (p == null) {
            return "💰 <b>P&amp;L ещё не посчитан</b>\nПервый расчёт — после первой сверки,\nдальше раз в 10 мин.";
        }
        if (!p.known()) {
            return "🔴 <b>P&amp;L не посчитан</b> — биржа не ответила в " + time(p.failedAt())
                    + "\nСледующая попытка — через 10 мин.";
        }
        double wallet = s.account() == null ? Double.NaN : s.account().wallet();
        StringBuilder sb = new StringBuilder("💰 <b>Сегодня ").append(money(p.today())).append("</b>");
        String todayPct = share(p.today(), wallet);
        if (!todayPct.isEmpty()) sb.append(" (").append(todayPct).append(')');
        sb.append("\n<pre>");
        pnlRow(sb, "Сегодня", p.today(), wallet, true);
        pnlRow(sb, "7 дней", p.week(), wallet, false);
        pnlRow(sb, "30 дней", p.month(), wallet, false);
        if (s.account() != null) {
            sb.append('\n').append(padRight("Открытые", LABEL)).append(padLeft(money(s.account().unrealized()), 8));
        }
        sb.append("</pre>");
        sb.append("\n<i>Нетто: сделки + комиссии + фандинг,\nбез разбивки. День — по Варшаве.</i>");
        if (p.failedAt() != null) {
            sb.append("\n🟠 Обновить не удалось в ").append(time(p.failedAt()))
                    .append(" — цифры от ").append(time(p.computedAt()));
        }
        return sb.append("\n<i>Посчитано ").append(ago(p.computedAt(), now)).append(" · ")
                .append(time(p.computedAt())).append("</i>").toString();
    }

    private static void pnlRow(StringBuilder sb, String label, double usd, double wallet, boolean first) {
        if (!first) sb.append('\n');
        sb.append(padRight(label, LABEL)).append(padLeft(money(usd), 8));
        String share = share(usd, wallet);
        if (!share.isEmpty()) sb.append(' ').append(padLeft(share, 6));
    }

    /**
     * The result as a share of the balance the period started with (wallet minus the result). A
     * deposit or withdrawal inside the period skews it; the dollars above are the exact figure.
     */
    private static String share(double usd, double wallet) {
        if (!Double.isFinite(usd) || !Double.isFinite(wallet)) return "";
        double start = wallet - usd;
        if (start <= 0) return "";
        return pct(usd / start * 100.0);
    }

    // ─── Commands without data ───────────────────────────────────────────────────────────────

    static String menu(OperatorSnapshot s, Optional<String> haltReason, Instant now) {
        StringBuilder sb = new StringBuilder(stateLine(s, haltReason, now));
        if (s != null) {
            sb.append("\n📒 ").append(positions(s.positions().size()));
            if (s.pnl() != null && s.pnl().known()) sb.append(" · 💰 сегодня ").append(money(s.pnl().today()));
        }
        return sb.append("\nВыбери действие:").toString();
    }

    /**
     * Control and the one fallback screen (24.09). /book /pnl /queue /why still answer when typed -
     * harmless, read-only, and the owner's thumbs may remember them - but they are not advertised:
     * the lab bot's 📊 Панель is where those numbers live now.
     */
    static String help() {
        return "📊 <b>Команды</b>\n"
                + "/status — состояние бота\n"
                + "/halt — стоп новых входов\n"
                + "/resume — снять халт\n"
                + "/close ADAUSDT — закрыть одну\n"
                + "/close all — закрыть всё\n"
                + "/menu — кнопки\n"
                + "<i>Халт не трогает выходы: стопы,\nтейки и закрытия работают всегда.</i>\n"
                + PANEL_HINT;
    }

    // ─── /queue ──────────────────────────────────────────────────────────────────────────────

    /** Candidate rows shown; the scanner keeps up to 30, a phone screen fits about this many. */
    static final int QUEUE_ROWS = 12;

    /** The scanner's reason codes in the owner's words (SCANNER VIEW CONTRACT v1). */
    static String reasonLabel(String code) {
        return switch (code == null ? "" : code) {
            case "book_full" -> "нет места";
            case "too_wide" -> "стоп шире, чем тянет депозит";
            case "cooldown" -> "пауза после сделки";
            case "corr" -> "коррелирует с книгой";
            case "outside_pool" -> "вне пула";
            case "halt" -> "халт";
            case "regime" -> "режим рынка";
            default -> "другое";
        };
    }

    private static String trigLabel(String trig) {
        return switch (trig == null ? "" : trig) {
            case "trend" -> "тренд";
            case "dip" -> "откат";
            case "short" -> "шорт";
            case "" -> "—";
            default -> trig;
        };
    }

    /**
     * Who is waiting, and for what: the closes the loop is retrying, then what the scanner saw on
     * its last pass - the book's room, BTC against its average, and the symbols that passed an
     * entry gate and were not opened, grouped by the gate that stopped them. The first line is
     * the worst of it: no scanner data, a close stuck in retries, a silent scanner, then the count.
     */
    static String queue(OperatorSnapshot s, ScannerView.Read read, Instant now) {
        List<OperatorSnapshot.QueuedClose> closes = s == null ? List.of() : s.closeQueue();
        ScannerView v = read == null ? null : read.view();
        String problem = read == null ? "файла сканера нет" : read.problem();
        StringBuilder sb = new StringBuilder();
        boolean closesInHeadline = false;
        if (v == null) {
            boolean broken = read != null
                    && (read.status() == ScannerView.Status.GARBAGE || read.status() == ScannerView.Status.TOO_BIG);
            sb.append(broken ? "🔴 <b>Файл сканера не читается</b>" : "🟠 <b>Нет данных сканера</b>")
                    .append("\n").append(esc(problem));
        } else if (!closes.isEmpty()) {
            sb.append("🟠 <b>Закрытия в повторе: ").append(closes.size()).append("</b>");
            closesInHeadline = true;
        } else if (read.status() == ScannerView.Status.STALE) {
            sb.append("🟠 <b>").append(esc(capitalize(problem))).append("</b>");
        } else if (v.queue().isEmpty()) {
            sb.append("⏳ <b>Очередь пуста</b> — ждущих входа нет");
        } else {
            sb.append("⏳ <b>Ждут входа: ").append(v.queue().size()).append("</b>");
        }

        if (!closes.isEmpty()) {
            if (!closesInHeadline) sb.append("\n🟠 <b>Закрытия в повторе: ").append(closes.size()).append("</b>");
            sb.append("\n<pre>");
            for (int i = 0; i < closes.size(); i++) {
                OperatorSnapshot.QueuedClose c = closes.get(i);
                if (i > 0) sb.append('\n');
                Duration wait = c.nextTryAt() == null ? Duration.ZERO : Duration.between(now, c.nextTryAt());
                sb.append(esc(padRight(shortSymbol(c.symbol()), 8) + " " + padLeft(c.attemptsSpent() + "/" + c.maxAttempts(), 4)
                        + "  " + (wait.isNegative() || wait.isZero() ? "сейчас" : "через " + age(wait))));
            }
            sb.append("</pre>\n<i>Стоп на бирже стоит. Рестарт бота\nочищает очередь — тогда /close заново.</i>");
        }
        if (v == null) return sb.toString();

        if (read.status() == ScannerView.Status.STALE) {
            if (closesInHeadline) sb.append("\n🟠 ").append(esc(capitalize(problem)));
            sb.append("\n<i>Ниже — последний проход, ").append(TgFormat.dateTime(v.ts())).append(".</i>");
        } else if (closesInHeadline) {
            sb.append("\n⏳ Ждут входа: ").append(v.queue().size());
        }
        sb.append("\nПроход ").append(time(v.ts())).append(" · ").append(ago(v.ts(), now));
        if (v.nextPassEta() != null && v.nextPassEta().isAfter(now)) {
            sb.append(" · след. ~").append(time(v.nextPassEta()));
        }
        sb.append("\n<pre>");
        String book = v.held() == null ? "—" : v.held() + (v.maxPositions() == null ? "" : "/" + v.maxPositions());
        if (v.room() != null) book += " · мест " + v.room();
        row(sb, "Книга", esc(book), true);
        ScannerView.Btc btc = v.btc();
        if (btc != null && btc.price() > 0) {
            String vsSma = btc.sma50() > 0 ? " " + pct((btc.price() / btc.sma50() - 1.0) * 100.0) + " к SMA50" : "";
            row(sb, "BTC", TgFormat.price(btc.price()) + vsSma, false);
        }
        String gate = btc == null || btc.shortGateOpen() == null ? "гейт —"
                : btc.shortGateOpen() ? "гейт открыт" : "гейт закрыт";
        row(sb, "Шорт", (v.shortArmed() ? "вкл" : "выкл") + " · " + gate, false);
        if (v.entryOk() != null || v.holdOk() != null) {
            row(sb, "Сигналы", "вход " + (v.entryOk() == null ? "—" : v.entryOk())
                    + " · держать " + (v.holdOk() == null ? "—" : v.holdOk()), false);
        }
        sb.append("</pre>");
        if (v.halted()) sb.append("\n⏸ Сканер видит халт — входов не пишет");

        // Grouped by the gate, in the scanner's own order: the first group is what most blocks entries.
        java.util.LinkedHashMap<String, List<ScannerView.Candidate>> groups = new java.util.LinkedHashMap<>();
        int shown = 0;
        for (ScannerView.Candidate c : v.queue()) {
            if (shown >= QUEUE_ROWS) break;
            String label = reasonLabel(c.reason());
            if (label.equals("другое") && !c.note().isEmpty()) label = c.note();
            groups.computeIfAbsent(label, k -> new java.util.ArrayList<>()).add(c);
            shown++;
        }
        for (java.util.Map.Entry<String, List<ScannerView.Candidate>> g : groups.entrySet()) {
            sb.append("\n<b>").append(esc(g.getKey())).append("</b> · ").append(g.getValue().size()).append("\n<pre>");
            sb.append(esc(padRight("Монета", 8) + " · " + padLeft("30д", 6) + " " + padLeft("Объём", 5)
                    + " " + padLeft("Стоп", 6)));
            for (ScannerView.Candidate c : g.getValue()) sb.append('\n').append(esc(candidateRow(c)));
            sb.append("</pre>");
        }
        if (v.queue().size() > shown) sb.append("\n… и ещё ").append(v.queue().size() - shown);

        List<String> pass = new java.util.ArrayList<>();
        if (!v.opened().isEmpty()) {
            pass.add("открыл " + String.join(", ", v.opened().stream().map(o -> esc(shortSymbol(o.symbol()))).toList()));
        }
        if (!v.closed().isEmpty()) {
            pass.add("закрыл " + String.join(", ", v.closed().stream()
                    .map(c -> esc(shortSymbol(c.symbol())) + closeLabel(c.reason())).toList()));
        }
        if (!pass.isEmpty()) sb.append("\nЭтот проход: ").append(String.join(" · ", pass));
        if (!v.why().isEmpty()) {
            sb.append("\n<i>Почему вошли: /why ").append(esc(shortSymbol(v.why().keySet().iterator().next())))
                    .append("</i>");
        }
        return sb.toString();
    }

    /** {@code TIA      L +18.0%  ×2.1  13.1%} - 30 columns: coin, side, 30d return, volume, stop width. */
    static String candidateRow(ScannerView.Candidate c) {
        String side = c.side().startsWith("S") ? "S" : "L";
        String vol = Double.isFinite(c.volRatio()) ? String.format(java.util.Locale.ROOT, "×%.1f", c.volRatio()) : "—";
        String stop = Double.isFinite(c.stopFrac())
                ? String.format(java.util.Locale.ROOT, "%.1f%%", Math.abs(c.stopFrac()) * 100.0) : "—";
        return padRight(shortSymbol(c.symbol()), 8) + " " + side + " " + padLeft(pct(c.ret30() * 100.0), 6)
                + " " + padLeft(vol, 5) + " " + padLeft(stop, 6);
    }

    private static String closeLabel(String reason) {
        return switch (reason == null ? "" : reason) {
            case "max-hold" -> " (срок)";
            case "exit-band" -> " (тренд)";
            default -> "";
        };
    }

    private static String capitalize(String s) {
        return s == null || s.isEmpty() ? "" : Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    // ─── /why ────────────────────────────────────────────────────────────────────────────────

    /**
     * Why the scanner opened this coin - the numbers it saw at the entry - and where the position
     * stands now. A position the scanner did not open (a hand trade, one adopted from the ledger)
     * or a missing file says so instead of inventing a reason.
     */
    static String why(String symbol, OperatorSnapshot s, ScannerView.Read read, Instant now) {
        OperatorSnapshot.Position p = s == null ? null
                : s.positions().stream().filter(x -> x.symbol().equals(symbol)).findFirst().orElse(null);
        ScannerView v = read == null ? null : read.view();
        ScannerView.Why w = v == null ? null : v.why().get(symbol);
        String coin = esc(shortSymbol(symbol));
        StringBuilder sb = new StringBuilder();
        if (w == null) {
            sb.append("🟠 <b>").append(coin).append(p == null ? " не в книге" : "").append("</b>")
                    .append("\nнет данных сканера о входе");
            if (v == null) sb.append("\n<i>").append(esc(read == null ? "файла сканера нет" : read.problem())).append("</i>");
            else if (p != null) sb.append("\n<i>Открыта не сканером (вручную или\nдо его памяти) либо уже забыта.</i>");
        } else {
            String side = w.side().isEmpty() ? (p == null ? "" : p.side().name()) : w.side();
            sb.append("📒 <b>").append(coin).append(side.isEmpty() ? "" : " · " + esc(side)).append(" · почему вошли</b>");
            sb.append("\n<pre>");
            row(sb, "Сигнал", esc(trigLabel(w.trig())) + " · " + TgFormat.dateTime(w.openedAt()), true);
            row(sb, "Цена входа", TgFormat.price(w.price()), false);
            row(sb, "За 30д", pct(w.ret30() * 100.0), false);
            row(sb, "От макс20д", pct(-w.fromHigh20() * 100.0), false);
            row(sb, "От макс90д", pct(-w.fromHigh90() * 100.0), false);
            row(sb, "Объём", Double.isFinite(w.volRatio())
                    ? String.format(java.util.Locale.ROOT, "×%.1f к среднему", w.volRatio()) : "—", false);
            double stopSign = side.startsWith("S") ? 1.0 : -1.0;
            row(sb, "Стоп", pct(stopSign * Math.abs(w.stopFrac()) * 100.0), false);
            sb.append("</pre>");
        }
        if (p != null) {
            sb.append("\nСейчас: ").append(pct(p.pnlPct()));
            if (Double.isFinite(p.unrealizedUsd())) sb.append(" · ").append(money(p.unrealizedUsd()));
            sb.append("\nдо стопа ").append(pct(p.toStopPct())).append(" · до тейка ").append(pct(p.toTakePct()));
            if (p.openedAt() != null) sb.append("\nдержит ").append(TgFormat.held(Duration.between(p.openedAt(), now)));
        }
        if (v != null) {
            sb.append("\n<i>Сканер: ").append(ago(v.ts(), now)).append(" · ").append(time(v.ts()));
            if (read.status() == ScannerView.Status.STALE) sb.append(" — молчит");
            sb.append("</i>");
        }
        return sb.toString();
    }

    /** The close-all confirmation: exactly what will be closed, and for how long the button lives. */
    /**
     * ▶️'s confirmation: the reason being lifted, and what lifting it does. The reason is escaped;
     * it may quote an exchange's HTML error page.
     */
    static String resumePrompt(String reason, boolean observeMode, long ttlSeconds) {
        return "⏸ <b>Снять халт?</b>\nПричина: " + esc(reason)
                + (observeMode ? "\n👁 Площадка в наблюдении:\nвходы останутся выключены."
                               : "\nВходы включатся со следующего\nсигнала сканера.")
                + "\n<i>Кнопка живёт " + ttlSeconds + " с.</i>";
    }

    /**
     * Empty when a queued close runs within seconds; otherwise why it will not. The loop executes
     * closes, and during an exchange hold it is parked in the rate limiter for the hold's length -
     * an IP ban can last hours. "Within seconds" then was a promise the owner would act on.
     */
    static String closeDelayNote(OperatorSnapshot s, Instant now) {
        if (s == null) return "";
        if (s.exchangeHoldMs() > 0) {
            return "\n🔴 Биржа на паузе ещё " + age(Duration.ofMillis(s.exchangeHoldMs()))
                    + ":\nзакрытия пройдут после паузы.\nБыстрее — вручную в приложении.";
        }
        Duration silent = Duration.between(s.at(), now);
        if (silent.compareTo(STALE_AFTER) > 0) {
            return "\n🔴 Цикл молчит " + age(silent) + ":\nзакрытия пройдут, когда оживёт.\n"
                    + "Быстрее — вручную в приложении.";
        }
        return "";
    }

    static String closeAllPrompt(List<OperatorSnapshot.Position> rows, Instant now, long ttlSeconds) {
        StringBuilder sb = new StringBuilder("🧯 <b>Закрыть всё: ").append(positions(rows.size())).append("?</b>");
        sb.append("\n<pre>");
        double total = 0;
        boolean anyPnl = false;
        for (int i = 0; i < rows.size(); i++) {
            OperatorSnapshot.Position p = rows.get(i);
            if (i > 0) sb.append('\n');
            sb.append(esc(padRight(shortSymbol(p.symbol()), 8) + " " + (p.side() == com.bot.core.Side.LONG ? "L" : "S")
                    + " " + padLeft(pct(p.pnlPct()), 6) + " " + padLeft(money(p.unrealizedUsd()), 8)));
            if (Double.isFinite(p.unrealizedUsd())) {
                total += p.unrealizedUsd();
                anyPnl = true;
            }
        }
        sb.append("</pre>");
        if (anyPnl) sb.append("\n💰 Нереал. сейчас: ").append(money(total));
        return sb.append("\nПо рынку, reduce-only; стопы и тейки\nснимутся вместе с позициями.")
                .append("\nОткрытое после этого списка\nтоже закроется.")
                .append("\n<i>Кнопка живёт ").append(ttlSeconds).append(" с.</i>").toString();
    }
}
