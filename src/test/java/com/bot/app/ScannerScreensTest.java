package com.bot.app;

import com.bot.core.Side;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * /queue and /why: what the scanner saw, in the owner's words. The first line is the worst of it,
 * every table fits a phone, and a missing or broken file is said out loud rather than drawn empty.
 */
class ScannerScreensTest {

    private static final Instant NOW = ScannerViewTest.PASS.plusSeconds(12 * 60);

    private static ScannerView.Read ok(JSONObject doc) {
        return new ScannerView.Read(ScannerView.Status.OK, ScannerView.parse(doc.toString()), "");
    }

    private static OperatorSnapshot snap(List<OperatorSnapshot.Position> rows, List<OperatorSnapshot.QueuedClose> closes) {
        return new OperatorSnapshot(NOW, NOW.minusSeconds(7200), "b", false, false, 0, false, false, 0,
                closes.size(), NOW, new OperatorSnapshot.Account(143.10, 120.70, 1.23), rows, true, null, closes);
    }

    private static OperatorSnapshot.Position tia() {
        return new OperatorSnapshot.Position("TIAUSDT", Side.LONG, 10, 0.5, 0.51, 0.1, 0.45, 0.6,
                NOW.minusSeconds(14 * 3600), true);
    }

    /** Every line inside a pre block: the part of the screen that must fit a phone. */
    private static List<String> preLines(String html) {
        List<String> out = new ArrayList<>();
        int from = 0;
        while (true) {
            int open = html.indexOf("<pre>", from);
            if (open < 0) return out;
            int close = html.indexOf("</pre>", open);
            for (String line : html.substring(open + 5, close).split("\n")) out.add(TgFormat.plain(line));
            from = close + 6;
        }
    }

    @Test
    @DisplayName("/queue: a count first, the book and BTC, candidates grouped by the gate that stopped them")
    void queueScreen() {
        String html = OperatorViews.queue(snap(List.of(), List.of()), ok(ScannerViewTest.doc()), NOW);

        assertTrue(html.startsWith("⏳ <b>Ждут входа: 2</b>"), html);
        assertTrue(html.contains("Проход 14:42 · 12 мин назад · след. ~15:42"), html);
        assertTrue(html.contains("15/15 · мест 0"), html);
        assertTrue(html.contains("86437 +16.6% к SMA50"), html);
        assertTrue(html.contains("вкл · гейт закрыт"), html);
        assertTrue(html.contains("<b>нет места</b> · 1"), html);
        assertTrue(html.contains("<b>стоп шире, чем тянет депозит</b> · 1"), html);
        assertTrue(html.contains("X        L +12.3%  ×2.1  13.1%"), html);
        assertTrue(html.contains("WIDE     L +40.0%     —  22.1%"), html);
        assertTrue(html.contains("Этот проход: открыл TIA · закрыл ADA (срок)"), html);
        for (String line : preLines(html)) assertTrue(line.length() <= 34, "wider than a phone: [" + line + "]");
    }

    @Test
    @DisplayName("every reason code has a Russian label; an unknown one is 'другое' and shows the scanner's note")
    void reasonLabels() {
        assertEquals("нет места", OperatorViews.reasonLabel("book_full"));
        assertEquals("стоп шире, чем тянет депозит", OperatorViews.reasonLabel("too_wide"));
        assertEquals("пауза после сделки", OperatorViews.reasonLabel("cooldown"));
        assertEquals("коррелирует с книгой", OperatorViews.reasonLabel("corr"));
        assertEquals("вне пула", OperatorViews.reasonLabel("outside_pool"));
        assertEquals("халт", OperatorViews.reasonLabel("halt"));
        assertEquals("режим рынка", OperatorViews.reasonLabel("regime"));
        assertEquals("другое", OperatorViews.reasonLabel("other"));
        assertEquals("другое", OperatorViews.reasonLabel("something-new"));

        JSONObject d = ScannerViewTest.doc().put("queue", new JSONArray().put(new JSONObject()
                .put("symbol", "A<B>&USDT").put("side", "SHORT").put("reason", "other").put("note", "эту <монету> берёт лонг")));
        String html = OperatorViews.queue(snap(List.of(), List.of()), ok(d), NOW);
        assertTrue(html.contains("<b>эту &lt;монету&gt; берёт лонг</b> · 1"), html);
        assertTrue(html.contains("A&lt;B&gt;&amp;"), "symbols are escaped: " + html);
        assertTrue(html.contains(" S "), "a short is marked S: " + html);
    }

    @Test
    @DisplayName("more than twelve waiting: twelve rows and a count of the rest")
    void queueIsCapped() {
        JSONArray q = new JSONArray();
        for (int i = 0; i < 20; i++) q.put(new JSONObject().put("symbol", "C" + i + "USDT").put("reason", "book_full"));
        String html = OperatorViews.queue(snap(List.of(), List.of()), ok(ScannerViewTest.doc().put("queue", q)), NOW);
        assertTrue(html.startsWith("⏳ <b>Ждут входа: 20</b>"), html);
        assertTrue(html.contains("<b>нет места</b> · 12"), html);
        assertTrue(html.contains("… и ещё 8"), html);
    }

    @Test
    @DisplayName("an empty queue says so, and a halt the scanner saw is named")
    void emptyQueueAndHalt() {
        JSONObject d = ScannerViewTest.doc().put("queue", new JSONArray()).put("halted", true);
        String html = OperatorViews.queue(snap(List.of(), List.of()), ok(d), NOW);
        assertTrue(html.startsWith("⏳ <b>Очередь пуста</b>"), html);
        assertTrue(html.contains("⏸ Сканер видит халт"), html);
    }

    @Test
    @DisplayName("closes stuck in retries outrank the scanner in the headline and list attempts and the wait")
    void closesRetrying() {
        OperatorSnapshot.QueuedClose ada = new OperatorSnapshot.QueuedClose("ADAUSDT", "max-hold", 2, 8, NOW.plusSeconds(40));
        String html = OperatorViews.queue(snap(List.of(), List.of(ada)), ok(ScannerViewTest.doc()), NOW);
        assertTrue(html.startsWith("🟠 <b>Закрытия в повторе: 1</b>"), html);
        assertTrue(html.contains("ADA       2/8  через 40 с"), html);
        assertTrue(html.contains("⏳ Ждут входа: 2"), html);
    }

    @Test
    @DisplayName("stale, missing and broken files are the headline - stale still shows its data")
    void fileProblems() {
        ScannerView stale = ScannerView.parse(ScannerViewTest.doc().toString());
        String s = OperatorViews.queue(snap(List.of(), List.of()),
                new ScannerView.Read(ScannerView.Status.STALE, stale, "сканер молчит 3 ч"), NOW);
        assertTrue(s.startsWith("🟠 <b>Сканер молчит 3 ч</b>"), s);
        assertTrue(s.contains("Ниже — последний проход, 23.09 14:42"), s);
        assertTrue(s.contains("<b>нет места</b>"), s);

        String missing = OperatorViews.queue(null,
                new ScannerView.Read(ScannerView.Status.MISSING, null, "файла сканера нет"), NOW);
        assertEquals("🟠 <b>Нет данных сканера</b>\nфайла сканера нет", missing);

        String broken = OperatorViews.queue(null,
                new ScannerView.Read(ScannerView.Status.GARBAGE, null, "файл сканера испорчен — не разбирается"), NOW);
        assertTrue(broken.startsWith("🔴 <b>Файл сканера не читается</b>"), broken);
    }

    @Test
    @DisplayName("/why: the numbers the scanner saw at the entry, then where the position stands now")
    void whyScreen() {
        String html = OperatorViews.why("TIAUSDT", snap(List.of(tia()), List.of()), ok(ScannerViewTest.doc()), NOW);

        assertTrue(html.startsWith("📒 <b>TIA · LONG · почему вошли</b>"), html);
        assertTrue(html.contains("тренд · 23.09 14:05"), html);
        assertTrue(html.contains("0.5058"), html);
        assertTrue(html.contains("За 30д     +18.0%"), html);
        assertTrue(html.contains("От макс20д −1.0%"), html);
        assertTrue(html.contains("От макс90д −5.0%"), html);
        assertTrue(html.contains("×2.4 к среднему"), html);
        assertTrue(html.contains("Стоп       −12.9%"), html);
        assertTrue(html.contains("Сейчас: +2.0% · +$0.10"), html);
        assertTrue(html.contains("держит 14 ч"), html);
        for (String line : preLines(html)) assertTrue(line.length() <= 34, "wider than a phone: [" + line + "]");
    }

    @Test
    @DisplayName("/why for a coin the scanner did not open, or with no file, says 'нет данных сканера о входе'")
    void whyWithoutData() {
        String hand = OperatorViews.why("ADAUSDT", snap(List.of(OperatorChannelTest.position("ADAUSDT", Side.LONG, 1.0, 1.01)),
                List.of()), ok(ScannerViewTest.doc()), NOW);
        assertTrue(hand.startsWith("🟠 <b>ADA</b>\nнет данных сканера о входе"), hand);
        assertTrue(hand.contains("Открыта не сканером"), hand);
        assertTrue(hand.contains("Сейчас: +1.0%"), "the live numbers are still there: " + hand);

        String none = OperatorViews.why("XRPUSDT", null,
                new ScannerView.Read(ScannerView.Status.MISSING, null, "файла сканера нет"), NOW);
        assertEquals("🟠 <b>XRP не в книге</b>\nнет данных сканера о входе\n<i>файла сканера нет</i>", none);
    }

    @Test
    @DisplayName("24.09: /book no longer promises coin buttons - they went with the duplicates")
    void bookHasNoCoinButtons() {
        List<OperatorSnapshot.Position> rows = new ArrayList<>();
        for (int i = 0; i < 15; i++) rows.add(OperatorChannelTest.position("C" + i + "USDT", Side.LONG, 1.0, 1.0));
        String book = OperatorViews.book(snap(rows, List.of()), NOW);

        assertFalse(book.contains("Кнопка монеты"), book);
        assertTrue(book.contains("C14"), "every coin is still on the screen");
        for (List<OperatorViews.Button> row : OperatorViews.MENU) {
            for (OperatorViews.Button b : row) {
                assertFalse(b.data().startsWith(OperatorViews.CB_WHY), "no coin button in the menu");
                assertTrue(b.data().getBytes(StandardCharsets.UTF_8).length <= 64);
            }
        }
    }
}
