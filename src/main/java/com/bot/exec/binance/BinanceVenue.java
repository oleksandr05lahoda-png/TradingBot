package com.bot.exec.binance;

import com.bot.core.Preconditions;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The only place in this repository where a Binance base URL exists — demo or production.
 * {@code VenueContainmentTest} fails the build if a production host appears in any other file.
 *
 * <p>The demo venue is the default and needs no configuration. The production venue is reachable
 * through exactly one gate, {@link #resolve(Map)}: {@code REAL_TRADING} must be the exact string
 * {@code ARMED} and the real credentials must be present under their own names. Every deviation —
 * a typo in the flag, missing keys, keys under the testnet names — refuses to start rather than
 * silently picking a venue. Missing configuration can therefore never select the real exchange,
 * and mis-configuration can never trade at all.
 *
 * <p>The older {@code testnet.binancefuture.com} redirects, and a redirect breaks signed requests.
 */
public final class BinanceVenue {

    /** What an armed production venue is allowed to do this run. */
    public enum RealMode {
        /** Read the account, reconcile, accept closes — never open. The default for REAL. */
        OBSERVE,
        /** Full operation. Requires {@code REAL_MODE=trade}, set by the operator's hand. */
        TRADE
    }

    private static final String DEMO_REST = "https://demo-fapi.binance.com";
    private static final String REAL_REST = "https://fapi.binance.com";

    private final String name;
    private final String restBaseUrl;
    private final List<String> allowedHosts;
    private final String keyEnv;
    private final String secretEnv;
    private final boolean real;
    private final RealMode realMode;

    private BinanceVenue(String name, String restBaseUrl, List<String> allowedHosts,
                         String keyEnv, String secretEnv, boolean real, RealMode realMode) {
        this.name = name;
        this.restBaseUrl = restBaseUrl;
        this.allowedHosts = allowedHosts;
        this.keyEnv = keyEnv;
        this.secretEnv = secretEnv;
        this.real = real;
        this.realMode = realMode;
    }

    /** The demo venue: default, key names testnet-specific so a real key has nowhere to hide. */
    public static BinanceVenue demo() {
        return new BinanceVenue("DEMO", DEMO_REST,
                List.of("demo-fapi.binance.com", "demo-fstream.binance.com"),
                "BINANCE_TESTNET_API_KEY", "BINANCE_TESTNET_API_SECRET",
                false, RealMode.TRADE);
    }

    /** Reads the process environment. See {@link #resolve(Map)} for the arming contract. */
    public static BinanceVenue resolveFromEnvironment() {
        return resolve(System.getenv());
    }

    /**
     * The single gate to the production venue. Fail-closed on every edge:
     * <ul>
     *   <li>{@code REAL_TRADING} unset or blank → demo, exactly as before this class existed;</li>
     *   <li>{@code REAL_TRADING=ARMED} (exact, case-sensitive) → real, requiring
     *       {@code BINANCE_REAL_API_KEY}/{@code BINANCE_REAL_API_SECRET};</li>
     *   <li>any other value → refuse to start: the operator reached for the switch and missed,
     *       and a missed switch must never quietly fall back to either venue;</li>
     *   <li>{@code REAL_MODE}: unset or {@code observe} → OBSERVE; {@code trade} → TRADE;
     *       anything else → refuse to start.</li>
     * </ul>
     */
    public static BinanceVenue resolve(Map<String, String> env) {
        Preconditions.notNull(env, "env");
        String arm = env.getOrDefault("REAL_TRADING", "").trim();
        if (arm.isEmpty()) return demo();
        if (!arm.equals("ARMED")) {
            throw new IllegalStateException(
                    "REAL_TRADING is set to \"" + arm + "\". The only value that arms the real exchange"
                            + " is exactly ARMED. Unset it to run against the demo venue; there is no"
                            + " third option and typos do not get to guess.");
        }
        String key = env.getOrDefault("BINANCE_REAL_API_KEY", "").trim();
        String secret = env.getOrDefault("BINANCE_REAL_API_SECRET", "").trim();
        if (key.isEmpty() || secret.isEmpty()) {
            throw new IllegalStateException(
                    "REAL_TRADING=ARMED but real credentials are missing.\n"
                            + "  Put them in local.env (git-ignored) as two lines:\n"
                            + "      BINANCE_REAL_API_KEY=...\n"
                            + "      BINANCE_REAL_API_SECRET=...\n"
                            + "  Create the key on binance.com with WITHDRAWALS DISABLED and an IP\n"
                            + "  whitelist. The testnet key names are not accepted here on purpose.");
        }
        String mode = env.getOrDefault("REAL_MODE", "").trim().toLowerCase(Locale.ROOT);
        RealMode realMode = switch (mode) {
            case "", "observe" -> RealMode.OBSERVE;
            case "trade" -> RealMode.TRADE;
            default -> throw new IllegalStateException(
                    "REAL_MODE is set to \"" + mode + "\" — the only values are observe (default) and trade.");
        };
        return new BinanceVenue("REAL", REAL_REST,
                List.of("fapi.binance.com", "fstream.binance.com"),
                "BINANCE_REAL_API_KEY", "BINANCE_REAL_API_SECRET",
                true, realMode);
    }

    public String name() { return name; }

    public String restBaseUrl() { return restBaseUrl; }

    public boolean isReal() { return real; }

    /** Meaningful only when {@link #isReal()}; demo is always TRADE. */
    public RealMode realMode() { return realMode; }

    /** Environment variable holding this venue's API key. */
    public String keyEnv() { return keyEnv; }

    /** Environment variable holding this venue's API secret. */
    public String secretEnv() { return secretEnv; }

    /** Host of {@link #restBaseUrl()}, for logs and boot banners. */
    public String restHost() {
        return URI.create(restBaseUrl).getHost();
    }

    /**
     * Throws unless {@code url} points at one of THIS venue's hosts. Called on every request the
     * adapter builds, so a request built for the wrong venue dies before it leaves the process.
     */
    public URI require(String url) {
        Preconditions.notBlank(url, "url");
        URI uri = URI.create(url);
        String host = uri.getHost();
        Preconditions.require(host != null, "URL has no host: " + url);
        Preconditions.require(allowedHosts.contains(host.toLowerCase(Locale.ROOT)),
                "refusing to send to \"" + host + "\": this process is wired to the " + name
                        + " venue and nothing else. Allowed hosts: " + allowedHosts);
        return uri;
    }
}
