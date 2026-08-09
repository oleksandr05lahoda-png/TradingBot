package com.bot.exec.binance;

import com.bot.core.Preconditions;

import java.net.URI;
import java.util.List;
import java.util.Locale;

/**
 * The only place in this repository where a Binance base URL exists.
 *
 * <p>No production endpoint, no flag selecting one, no environment variable that could introduce
 * one. The previous generation had exactly such a flag — {@code BINANCE_USE_TESTNET} defaulting to
 * {@code 0} — so an unset variable on a fresh deployment silently selected the real exchange:
 * absence of configuration selected real money. The mainnet host is unreachable here because it is
 * not written down, and {@code NoProductionEndpointTest} fails the build if it ever is.
 *
 * <p>{@link #ALLOWED_HOSTS} exists so a future host migration stays a one-line change that still
 * cannot land on production. (The older {@code testnet.binancefuture.com} redirects, and a redirect
 * breaks signed requests.)
 */
public final class BinanceTestnetEndpoint {

    /** USDⓈ-M futures testnet REST base. */
    public static final String REST_BASE_URL = "https://demo-fapi.binance.com";

    /** USDⓈ-M futures testnet websocket base. Present for completeness; the REST path is authoritative. */
    public static final String WEBSOCKET_BASE_URL = "wss://demo-fstream.binance.com";

    /**
     * Hosts this system will talk to. A whitelist, not a blacklist: anything not named here is
     * refused, so a mistake fails closed instead of routing orders somewhere unexpected.
     */
    public static final List<String> ALLOWED_HOSTS = List.of(
            "demo-fapi.binance.com",
            "demo-fstream.binance.com");

    private BinanceTestnetEndpoint() {}

    /** Host of {@link #REST_BASE_URL}, for logs and boot banners. */
    public static String restHost() {
        return URI.create(REST_BASE_URL).getHost();
    }

    /**
     * Throws unless {@code url} points at a whitelisted testnet host. Called on every request the
     * adapter builds, so a URL assembled from parts cannot drift off the testnet even if some future
     * edit builds it from a string that came from outside.
     */
    public static URI requireTestnet(String url) {
        Preconditions.notBlank(url, "url");
        URI uri = URI.create(url);
        String host = uri.getHost();
        Preconditions.require(host != null, "URL has no host: " + url);
        Preconditions.require(ALLOWED_HOSTS.contains(host.toLowerCase(Locale.ROOT)),
                "refusing to send to \"" + host + "\": this build talks to the Binance futures testnet "
                        + "and nothing else. Allowed hosts: " + ALLOWED_HOSTS);
        return uri;
    }
}
