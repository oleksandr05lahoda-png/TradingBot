package com.bot.exec.binance;

import com.bot.core.Preconditions;

import java.net.URI;
import java.util.List;
import java.util.Locale;

/**
 * The only place in this repository where a Binance base URL exists. Deliberately no production host
 * and no flag selecting one, so missing configuration cannot select the real exchange;
 * {@code NoProductionEndpointTest} fails the build if a mainnet host is ever written down.
 *
 * <p>The older {@code testnet.binancefuture.com} redirects, and a redirect breaks signed requests.
 */
public final class BinanceTestnetEndpoint {

    /** USDⓈ-M futures testnet REST base. */
    public static final String REST_BASE_URL = "https://demo-fapi.binance.com";

    /** USDⓈ-M futures testnet websocket base; the REST path is the authoritative one. */
    public static final String WEBSOCKET_BASE_URL = "wss://demo-fstream.binance.com";

    /**
     * Whitelist, not a blacklist: an unlisted host is refused, so mistakes fail closed. A constant
     * so a future host migration is one line that still cannot land on production.
     */
    public static final List<String> ALLOWED_HOSTS = List.of(
            "demo-fapi.binance.com",
            "demo-fstream.binance.com");

    private BinanceTestnetEndpoint() {}

    /** Host of {@link #REST_BASE_URL}, for logs and boot banners. */
    public static String restHost() {
        return URI.create(REST_BASE_URL).getHost();
    }

    /** Throws unless {@code url} points at a whitelisted host. Called on every request the adapter builds. */
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
