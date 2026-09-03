package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.risk.TradePlan;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * What this process was about to do to a symbol, written down BEFORE the entry is sent and erased
 * only once the protective stop rests (or the position is confirmed gone). The window between a
 * market fill and its stop is the one place the book knows nothing: a crash there, or an entry
 * whose response was lost and that filled anyway, used to reach the next pass as an
 * {@code UNKNOWN_POSITION} with no risk on record — which the stop repair then refused to touch,
 * for ever (audit 03.09). With the intent on disk the reconciler can name the stop that was meant
 * and place it, instead of asking a human to.
 *
 * <p>Best-effort persistence: a failed write is logged and the intent still lives in memory, so
 * the in-process paths never depend on the disk.
 */
public final class EntryIntents {

    private static final Logger LOG = Logger.getLogger(EntryIntents.class.getName());

    /** An intent this old is not evidence about anything still open: ignored and pruned. */
    public static final long MAX_AGE_MS = 24 * 3_600_000L;

    public record Intent(String symbol, Side side, BigDecimal quantity, BigDecimal stopPrice,
                         String signalId, long recordedAtMs) {
        public Intent {
            Preconditions.notBlank(symbol, "symbol");
            Preconditions.notNull(side, "side");
            Preconditions.notNull(quantity, "quantity");
            Preconditions.notNull(stopPrice, "stopPrice");
            Preconditions.notBlank(signalId, "signalId");
        }

        public boolean expired(long nowMs) {
            return nowMs - recordedAtMs > MAX_AGE_MS;
        }
    }

    private final Path path;
    private final Map<String, Intent> intents = new ConcurrentHashMap<>();

    private EntryIntents(Path path) {
        this.path = path;
    }

    /** No file: intents live only as long as the process — enough for the in-process repair. */
    public static EntryIntents inMemory() {
        return new EntryIntents(null);
    }

    /** Backed by a file next to the ledger; whatever a previous process left there is loaded. */
    public static EntryIntents at(Path path, long nowMs) {
        EntryIntents store = new EntryIntents(Preconditions.notNull(path, "path"));
        store.load(nowMs);
        return store;
    }

    public synchronized void record(TradePlan plan, long nowMs) {
        Preconditions.notNull(plan, "plan");
        intents.put(plan.symbol(), new Intent(plan.symbol(), plan.side(), plan.quantity(),
                plan.stopPrice(), plan.signalId(), nowMs));
        persist();
    }

    public synchronized void clear(String symbol) {
        if (intents.remove(symbol) != null) persist();
    }

    /** A live intent for the symbol, if one is on record and not expired. */
    public Optional<Intent> get(String symbol, long nowMs) {
        Intent intent = intents.get(symbol);
        if (intent == null || intent.expired(nowMs)) return Optional.empty();
        return Optional.of(intent);
    }

    public List<Intent> all() {
        return new ArrayList<>(intents.values());
    }

    private void load(long nowMs) {
        if (path == null || !Files.exists(path)) return;
        try {
            JSONObject root = new JSONObject(Files.readString(path, StandardCharsets.UTF_8));
            JSONArray rows = root.optJSONArray("intents");
            if (rows == null) return;
            int loaded = 0;
            for (int i = 0; i < rows.length(); i++) {
                JSONObject row = rows.getJSONObject(i);
                Intent intent = new Intent(row.getString("symbol"), Side.valueOf(row.getString("side")),
                        new BigDecimal(row.getString("quantity")), new BigDecimal(row.getString("stopPrice")),
                        row.getString("signalId"), row.getLong("recordedAtMs"));
                if (intent.expired(nowMs)) continue;
                intents.put(intent.symbol(), intent);
                loaded++;
            }
            if (loaded > 0) {
                LOG.warning("[Intents] " + loaded + " entry intent(s) survived the last process: "
                        + intents.keySet() + " — the reconciler will place their stops if the positions exist");
            }
        } catch (RuntimeException | IOException e) {
            LOG.warning("[Intents] " + path + " is unreadable (" + e.getMessage() + ") — starting empty");
        }
    }

    private void persist() {
        if (path == null) return;
        JSONArray rows = new JSONArray();
        for (Intent intent : intents.values()) {
            rows.put(new JSONObject()
                    .put("symbol", intent.symbol())
                    .put("side", intent.side().name())
                    .put("quantity", intent.quantity().toPlainString())
                    .put("stopPrice", intent.stopPrice().toPlainString())
                    .put("signalId", intent.signalId())
                    .put("recordedAtMs", intent.recordedAtMs()));
        }
        String body = new JSONObject().put("intents", rows).toString();
        try {
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, body, StandardCharsets.UTF_8);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.warning("[Intents] could not persist to " + path + ": " + e.getMessage());
        }
    }
}
