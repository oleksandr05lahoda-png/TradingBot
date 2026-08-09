# Binance USDⓈ-M futures — testnet risk engine and execution layer

A foundation, not a trading system. It decides **how much** and **whether at all**, and it turns an
approved decision into orders that survive lost responses, partial fills and restarts. It does not
decide **what** to trade, and it cannot: there is no strategy in this repository, no indicator, and
no code path that produces a signal.

That is deliberate. Ninety-two strategy candidates in this project have been through pre-registered
out-of-sample, walk-forward and null-model gates, and none passed. Building the risk and execution
layer first means that when something eventually does pass, it inherits machinery whose correctness
is already established — rather than being wired to an executor that has to be trusted on the day it
first matters.

> **Testnet only.** The single base URL in this build is the Binance futures testnet. No production
> endpoint exists anywhere in the sources, and `NoProductionEndpointTest` fails the build if one
> appears. See [Testnet-only, structurally](#testnet-only-structurally).

---

## Contents

- [Quick start](#quick-start)
- [Testnet keys](#testnet-keys)
- [Code map](#code-map)
- [The risk formulas, with numbers](#the-risk-formulas-with-numbers)
- [Testnet-only, structurally](#testnet-only-structurally)
- [Execution guarantees](#execution-guarantees)
- [Configuration](#configuration)
- [Tests](#tests)
- [What this does not do](#what-this-does-not-do)

---

## Quick start

```bash
export BINANCE_TESTNET_API_KEY=...
export BINANCE_TESTNET_API_SECRET=...
./gradlew run
```

Then type a signal at the prompt:

```
BTCUSDT LONG entry=64000 stop=62800 lev=3
```

Non-interactive smoke run from a file — `example-signals.txt` ships with the repository and includes
one trade that is refused on purpose, so a single run exercises both outcomes:

```bash
./gradlew run --args="--script example-signals.txt"
```

Edit the prices to something near the current testnet mark before running. A stale entry price fills
far from where the plan assumed, and the coordinator closes the position again on slippage — correct
behaviour, but not the demonstration you wanted.

Drain the external queue instead of the console:

```bash
./gradlew run --args="--source supabase"
```

Requires JDK 21. If Gradle reports an invalid `JAVA_HOME`, point it at a real JDK 21 for the
command, e.g. `JAVA_HOME=~/.jdks/ms-21.0.9 ./gradlew run`.

### Manual signal format

```
SYMBOL SIDE entry=<price> [stop=<price>] [atr=<value>] [lev=<1..5>] [id=<text>]
```

At least one of `stop=` or `atr=` is required. A line with neither is refused at the point of
typing — there is nothing to size from and nothing to protect the position with. Blank lines and
lines starting with `#` are ignored. A malformed line is logged and skipped rather than fatal: a
typo must not take down a loop that is currently holding positions.

---

## Testnet keys

1. Open the Binance **futures testnet / demo trading** site and sign in.
2. Create an API key from *that* interface. A key created on the live exchange authenticates but
   has no testnet account behind it, which surfaces as a confusing `-1109 Invalid account`.
3. **Create the key with withdrawals DISABLED.** A bot needs read and trade permissions and nothing
   else. This is the cheapest and strongest safety measure available: a leaked trading key without
   withdrawal rights cannot move your money off the exchange. Add an IP allowlist as well if the
   bot runs from a fixed address.
4. Export the key and secret as environment variables. **Never** put them in a source file, a
   config file under version control, or a command that lands in your shell history:

```bash
export BINANCE_TESTNET_API_KEY=...
export BINANCE_TESTNET_API_SECRET=...
```

`BinanceSigner.fromEnvironment()` is the only way credentials enter this program, its `toString()`
deliberately reveals nothing, and no signature or secret is ever logged. There is no default, no
fallback and no constant anywhere in the tree that could hold a key.

---

## Code map

```
com.bot.core            pure value types, no I/O, no policy
  Side                  LONG/SHORT, sign(), stop and take-profit geometry
  InstrumentFilters     tickSize / stepSize / minQty / minNotional; BigDecimal quantisation
  Preconditions         fail-closed argument checks

com.bot.risk            the gate — pure arithmetic, no network, no clock of its own
  RiskConstants         MAX_LEVERAGE = 5, risk ceiling 1%, liquidation buffer floor 30%
  RiskConfig            everything configurable, bounded by RiskConstants
  StopLoss              sealed: Structural | AtrFallback — the reason a stopless position cannot exist
  PositionSizer         qty = balance x riskFraction / |entry - stop|. Pure and unclamped
  MarginTier            one maintenance-margin bracket (rate + maintenance amount)
  MarginTierTable       the brackets for a symbol, validated for contiguity and continuity
  LiquidationCalculator isolated USDⓈ-M liquidation price, Binance's formula, with bracket iteration
  LiquidationSafety     the >= 30% buffer invariant, and the highest leverage that would satisfy it
  TakeProfitPolicy      exits in R (default 1.5R / 2R), projected onto real lot sizes
  ExposureBook          what is open; long and short tracked separately and never netted
  DailyLossKillSwitch   latching daily loss limit on the UTC day
  RiskEngine            evaluate(request, balance, now) -> Approved(TradePlan) | Rejected(reason)
  TradePlan             an approved trade. Package-private constructor: only RiskEngine builds one

com.bot.signal          the only way a trade idea enters the system
  Signal                symbol, side, entry, optional structural stop, optional ATR, leverage
  SignalSource          interface — exactly two implementations, enforced by a test
  ManualTestnetInput      an operator typing
  SupabaseQueueSource     an external PostgREST queue

com.bot.exec            execution — the core knows no HTTP
  ExchangePort          the seam. Everything above depends on this and nothing else
  ExchangeSnapshots     OrderStatus / PositionSnapshot / AccountSnapshot — what the exchange says
  OrderRequest          one order, with the combinations the exchange accepts enforced in the type
  OrderTypes            OrderSide (BUY/SELL) deliberately distinct from Side (LONG/SHORT)
  ClientOrderIdFactory  deterministic ids — the whole of the idempotency story
  IdempotentOrderPlacer ask-before-sending, adopt duplicates, probe on ambiguity
  PreTradeValidator     tick / lot / notional / reduce-only checks before anything is sent
  RateLimiter           sliding-window weight and order-rate budgets
  ExecutionCoordinator  entry -> stop -> exits, sized from what actually filled
  Reconciler            exchange wins; drift halts trading
  DeadMansSwitch        exchange-side auto-cancel of working orders, never positions
  TradingHalt           a one-way latch that stops opening, never closing
  AlertSink             log, optional Telegram push, composite
  binance/
    BinanceTestnetEndpoint       the only base URL in the repository
    BinanceSigner                HMAC-SHA256, credentials from the environment only
    BinanceErrorCodes            the codes that change behaviour
    BinanceFuturesTestnetAdapter ExchangePort over testnet REST

com.bot.app
  TestnetBot            assembly and the loop. No risk arithmetic, no exchange knowledge
```

Data flow:

```
SignalSource ──▶ RiskEngine.evaluate ──▶ ExecutionCoordinator.execute ──▶ ExchangePort
                       │                          │                            ▲
                       └── ExposureBook ◀── Reconciler ─────────────────────────┘
                                                  │
                                           DeadMansSwitch
```

---

## The risk formulas, with numbers

### Position size comes from the stop

```
qty = balance × riskFraction / |entry − stop|
```

The direction of that arrow is the whole design. The stop is chosen first — by the structure the
signal carried, or by volatility — and the size is whatever makes the loss at that stop equal the
budgeted risk. There is no method anywhere that takes a size and returns a stop.

**Worked example.** Balance $10,000, risk 0.5%, entry 64,000, stop 62,800:

```
R          = |64,000 − 62,800| = 1,200
budget     = 10,000 × 0.005    = $50.00
qty        = 50 / 1,200        = 0.0416666… BTC
lot-aligned (step 0.001, always down) = 0.041 BTC
risk        = 0.041 × 1,200    = $49.20     (never more than the budget — rounding is always down)
notional    = 0.041 × 64,000   = $2,624.00
margin @3x  = 2,624 / 3        = $874.67
```

Leverage does not appear. It changes only how much margin is locked up, and therefore where the
position liquidates.

### Liquidation price — Binance's formula, not 1/leverage

Binance's general form, with *"in isolated margin mode, WB is `isolatedWalletBalance` of the isolated
position, TMM = 0, UPNL = 0"*, collapses for one isolated position to:

```
liq = (WB + cum − side × q × EP) / (q × MMR − side × q)          side = +1 long, −1 short
```

where `WB` is the isolated wallet balance, `EP` the entry price, `q` the size in base units, and
`MMR` / `cum` the maintenance margin rate and maintenance amount of the governing bracket.

It is derived rather than copied, so it can be checked. Liquidation is where equity meets the
maintenance requirement, `WB + uPnL = MM`, with `MM = q × P × MMR − cum`:

```
long :  WB + q(P − EP) = q·P·MMR − cum   →   P = (WB + cum − q·EP) / (q(MMR − 1))
short:  WB + q(EP − P) = q·P·MMR − cum   →   P = (WB + cum + q·EP) / (q(MMR + 1))
```

Both are the single expression above.

**Worked example.** Long 1 BTC at 100,000 with 5x isolated, fees ignored. Notional 100,000 falls in
the 25k–100k bracket of the bundled table: MMR 5%, cum 700. `WB = 100,000 / 5 = 20,000`.

```
liq = (20,000 + 700 − 100,000) / (1 × (0.05 − 1)) = −79,300 / −0.95 = 83,473.68
```

The naive `entry × (1 − 1/leverage)` would say 80,000. The real price is **higher** — liquidation
happens before the margin is fully gone, because the maintenance requirement bites first. The naive
number is always the optimistic one, and being optimistic about liquidation distance costs the whole
isolated margin instead of the planned R.

Two further details the code handles and a one-line formula does not:

- **Maintenance margin is piecewise linear**, not a flat percentage: `MM = notional × MMR − cum`.
  The subtracted maintenance amount is what makes it continuous across bracket boundaries.
  `MarginTierTable` refuses any table where it is not — a discontinuity means a wrong number, and a
  wrong maintenance margin is a wrong liquidation price.
- **The governing bracket depends on price**, which is what is being solved for. The solver
  re-selects the bracket at the price it computed and solves again until it settles; if it
  oscillates between two brackets it takes the stricter one, which lands nearer to entry.

The entry fee is subtracted from the isolated wallet balance before solving. It is small — 0.05% of
notional — but it moves liquidation *towards* entry, which is the direction it is dangerous to
ignore.

### The liquidation buffer, and why 30%

```
buffer = |stop − liq| / |entry − liq|   ≥ 0.30,   with the stop strictly between entry and liq
```

Equivalently: the stop may travel at most 70% of the way to liquidation.

**Worked example**, continuing the position above (entry 100,000, liq 83,473.68):

| stop   | distance left to liq | buffer         | verdict  |
|--------|----------------------|----------------|----------|
| 90,000 | 6,526.32 of 16,526.32| 39.5%          | accepted |
| 87,000 | 3,526.32 of 16,526.32| 21.3%          | rejected |
| 80,000 | past liquidation     | 0% (not inside)| rejected |

The margin is this wide because the stop and the liquidation are triggered by *different prices*.
The stop fires on the trigger price its order carries; liquidation fires on the exchange's mark
price, and on a thin book those two disagree by more than people expect. A stop that is merely
"before" liquidation gets overtaken by a mark excursion, and the position closes on the exchange's
terms rather than yours.

A rejection says what would have worked: *"the same trade passes at 2x or lower"*. That is
diagnostics, not an automatic retry — nothing re-runs the trade at lower leverage on its own.

### Take profits, in R

Default: half the position at 1.5R, half at 2R, both reduce-only. From the example above
(entry 64,000, stop 62,800, R = 1,200, qty 0.041):

```
1.5R → 64,000 + 1.5 × 1,200 = 65,800   qty 0.020
2.0R → 64,000 + 2.0 × 1,200 = 66,400   qty 0.021   (the last leg absorbs the rounding remainder)
```

The legs sum to exactly the position. If a split cannot be made in legal lot sizes, it collapses to
a single leg at the **nearest** R rather than being patched — patching would push size further out,
which is the wrong direction to err in when the requested profile is unachievable.

### Limits

| limit | default | measured in |
|---|---|---|
| risk per trade | 0.5% (hard cap 1%) | share of balance |
| notional per trade | 100% of balance | share of balance, plus an optional absolute ceiling |
| aggregate LONG exposure | 200% of balance | share of balance |
| aggregate SHORT exposure | 200% of balance | share of balance |
| concurrent positions | 3 | count |
| margin utilisation | 50% of balance | share of balance |
| daily loss | 3% of the day's opening balance | share of balance |
| leverage | 5x | hard constant, not configurable upward |

Every ceiling is a **share of capital**, not a dollar amount. A fixed dollar cap stops meaning
anything the moment the balance moves. The count-based version of this mistake is already recorded
in this repository's history: capping position *count* instead of exposure permits twenty correlated
alts at 20% of the account each — 400% of the account — while the dashboard reads "one position at a
time".

Long and short exposure are tracked separately and **never netted**. A 100k long and a 100k short
are not a flat book; they are two positions, two liquidation prices, two funding payments and two
ways to be wrong.

**The daily loss kill switch latches.** Once the day's loss crosses the limit, trading stops until
the next UTC day — it does not resume because the next mark tick made the number look better. What
it counts is `realised + min(0, unrealised)`: an open position at −8% blocks new entries before it
closes, and an open *winner* cannot offset a realised loss. The realised figure is re-seeded from the
exchange's own income ledger on every reconciliation pass, so a restart mid-drawdown cannot clear it.

---

## Testnet-only, structurally

The previous generation of this codebase selected its endpoint with `BINANCE_USE_TESTNET`, defaulting
to `0`. An unset variable on a fresh deployment silently selected the real exchange — absence of
configuration selected real money. That shape is not reproduced here.

- `BinanceTestnetEndpoint` holds the only base URL in the repository. The production host is not
  reachable because it is not written down.
- Every assembled request URL is re-checked against an allowlist by `requireTestnet(...)`, so a URL
  built from parts cannot drift off the testnet.
- `NoProductionEndpointTest` scans every `.java` file and every `*.gradle` file in the tree and fails
  the build if a production hostname appears — in code, in a string, or in a comment. It builds the
  forbidden names from fragments at runtime, so no production host is a literal in the test either
  and the test scans its own source like any other file.
- The scan is careful about a trap: `demo-fapi.<domain>` ends with the production `fapi.<domain>`,
  which ends with the spot `api.<domain>`. Naive substring matching would flag the legitimate
  testnet host, so each pattern is anchored to the start of a hostname label.

There is no flag, environment variable or configuration file that can point this build at production.

---

## Execution guarantees

**Idempotency.** Client order ids are a pure function of `(signalId, purpose, index)` — no timestamp,
no random suffix, no attempt counter. A network timeout is ambiguous: the order may have landed with
only the response lost, and a retry that invents a fresh id turns that ambiguity into two positions.
The protocol is: ask the exchange first; send; treat an error code as final; treat
`-4116 DUPLICATED_CLIENT_ORDER_ID` as the exchange saying *"I already have it"*; and on an ambiguous
failure, probe by id before resending — with the same id, so the duplicate check is still the
backstop.

**The stop goes on immediately.** Nothing is allowed between a confirmed fill and the protective
stop: not the slippage assessment, not the take-profit arithmetic, not the book update. If the stop
cannot be placed, the position is closed reduce-only and trading halts. The stop is a
`closePosition` `STOP_MARKET` triggering on **mark price** — the same price that liquidates the
position, and the one that stays correct after a take-profit leg has already shrunk the size.

**Partial fills size everything downstream.** Stops and exits are sized from what the exchange says
filled, never from what was requested. A stop sized for the intended quantity leaves the difference
unprotected while every log line reads as covered.

**Slippage can void a trade after it opens.** The plan's risk was computed against an intended entry.
If the fill lands far enough away that the real distance to the stop puts more than 120% of the
budgeted money at risk, the position is closed reduce-only rather than kept.

**Reconciliation: the exchange wins.** Positions and orders are compared with local belief on
start-up and on a timer. Any disagreement realigns the book *and* trips `TradingHalt`, because a
disagreement means an assumption was wrong and opening more positions on top of a wrong assumption is
how a small bug becomes an expensive one. The sharpest check is an open position with no working
stop — the state a crash between "entry filled" and "stop placed" would leave.

A halt stops **opening**. It never stops closing. A lock that seals positions in is not a safety
feature; the earlier version of this codebase learned that when a gate covering the whole cycle made
live positions unclosable and time-stops silently stopped working.

**Rate limits before sending, not after.** Weight per minute and order rate per 10s/minute are
enforced as sliding windows before each request, and the exchange's own `X-MBX-USED-WEIGHT-1M`
supersedes the local tally. A ban is a risk event, not an inconvenience: a `418` while a position is
open means the bot cannot place a stop, amend one or close, and it holds leveraged exposure it cannot
act on until the ban lifts.

**Precision before sending.** Tick, lot step, min/max quantity and minimum notional are all checked
locally against live `exchangeInfo` filters. Reduce-only exits are exempt from minimum notional, as
the exchange exempts them.

**Dead-man's switch.** The exchange-side `countdownCancelAll` is re-armed on a heartbeat for every
symbol holding a position. If the bot stops calling it, the **exchange** cancels that symbol's
working orders by itself; positions are untouched, because force-closing a position is a trading
decision and a watchdog does not get to make one. Server-side is the point: a watchdog thread inside
this process dies in the same crash that stranded the orders. The local half handles the other
failure — alive but unable to reach the exchange — by halting new risk and alerting loudly.

---

## Configuration

| variable | required | meaning |
|---|---|---|
| `BINANCE_TESTNET_API_KEY` | yes | testnet key, **withdrawals disabled** |
| `BINANCE_TESTNET_API_SECRET` | yes | testnet secret |
| `DEFAULT_LEVERAGE` | no (3) | used when a signal does not specify; capped at 5 |
| `SUPABASE_URL` | only for `--source supabase` | PostgREST base URL |
| `SUPABASE_QUEUE_KEY` | only for `--source supabase` | falls back to `SUPABASE_KEY` |
| `TELEGRAM_BOT_TOKEN` | no | enables push alerts |
| `TELEGRAM_CHAT_ID` | no | enables push alerts |

Risk parameters are code, not environment variables — `RiskConfig.defaults()`. A limit that can be
moved by an environment variable is not a limit, and the one that matters gets raised at exactly the
wrong moment. `RiskConfig` cannot be constructed with values that breach `RiskConstants`.

### The external queue contract

`--source supabase` drains `public.bot_orders`:

| column | meaning |
|---|---|
| `id` | row identity; the signal id and therefore the client order id derive from it |
| `symbol`, `side` | `LONG` / `SHORT` |
| `entry` | intended entry price |
| `sl` | structural stop; optional if `atr` is present |
| `atr` | optional volatility for the fallback stop |
| `leverage` | optional; capped at 5 |
| `status` | `pending` → `sent` (claimed) → `rejected` |
| `testnet` | must be `true`; a row that is not is refused, not routed |

A row is claimed with a conditional PATCH carrying `status=eq.pending`, so two processes polling the
same queue cannot both win it. A failed read **throws** rather than returning an empty list —
"the queue looked empty" and "the queue was unreachable" have to stay distinguishable. The status
column must accept `rejected`, or refused rows will be polled again.

---

## Tests

```bash
./gradlew test
```

156 tests. The property-based ones draw thousands of cases from a seeded PRNG so a failure is
reproducible; override the seed with `-Dbot.test.seed=123456`.

| requirement from the brief | test |
|---|---|
| property-based sizing: wider stop → smaller size | `PositionSizerPropertyTest` |
| property-based sizing: risk in $ constant for any input | `PositionSizerPropertyTest` |
| liquidation-buffer invariant | `LiquidationBufferInvariantTest` |
| idempotent resubmit | `IdempotentResubmitTest` |
| partial fill | `PartialFillTest` |
| reconciliation drift | `ReconciliationDriftTest` |
| no production URL | `NoProductionEndpointTest` |
| `MAX_LEVERAGE` | `MaxLeverageTest` |

Two of them are worth calling out because they assert about the system rather than about a function:

- `LiquidationBufferInvariantTest.noApprovedPlanEverViolatesTheInvariant` sweeps thousands of
  randomised inputs through the whole gate and asserts that **no plan it ever approves** breaches the
  buffer — and that enough plans are approved for the sweep to mean anything.
- `SignalSourceImplementationsTest` asserts that exactly two `SignalSource` implementations exist and
  that neither computes anything strategy-shaped. A third implementation has to delete this test to
  land, which makes it a conversation rather than an accident.

The end-to-end chain — typed signal → size from the stop → entry → stop and exits placed →
reconciliation converges — is `SmokeRunChainTest`, run against an in-memory exchange on every build.
Running the same chain against the real testnet additionally exercises the HTTP adapter, signing and
clock sync, which the in-memory version does not.

### What the tests do not cover

Everything below `ExchangePort` is exercised against a fake. The real testnet is what proves the
adapter: signing, clock drift, the actual JSON field names, rate-limit headers, and the specific
error codes the exchange returns. And testnet is not mainnet — liquidity, slippage, latency and
behaviour under load all differ. A green build proves the *path* is correct, not that it is
profitable and not how it behaves under real stress.

---

## What this does not do

- **It does not decide what to trade.** There is no strategy, no indicator, no signal generation, and
  the `SignalSource` interface has exactly two transports and no third.
- **It does not reach production.** No production endpoint exists in the sources, and a test fails
  the build if one appears.
- **It does not promise profit.** It is a risk and execution layer. Its correctness criterion is that
  it refuses correctly and executes exactly what was approved.
