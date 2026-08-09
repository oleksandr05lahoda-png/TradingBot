# Inventory and cleanup — branch `feature/testnet-risk-core`

Snapshot of what was in `src/main/java/com/bot` before this branch, what happened to each module, and
why. `main` is untouched; every deletion is one `git revert` away.

## What was there

| module | lines | what it was |
|---|---:|---|
| `SignalSender.java` | 3,842 | market data, websockets, funding/liquidation/listing capture, signal generation |
| `BinanceTradeExecutor.java` | 2,955 | order execution, position tracking, endpoint selection |
| `TradingCore.java` | 2,422 | indicator library: Keltner, MACD, ADX, StochRSI, Bollinger, volume profile, swing structure, order blocks, divergence, VSA, a forecast engine |
| `DecisionEngineMerged.java` | 1,771 | signal scoring, `TradeIdea` generation, coin categorisation |
| `RiskGuard.java` | 741 | pre-trade risk limits — **never called from anywhere** |
| `BotMain.java` | 699 | wiring and the Telegram entry point |
| `SupabaseSignalBridge.java` | 600 | drained `public.bot_orders` into the executor |
| `TelegramBotSender.java` | 408 | Telegram notifications |
| `RiskGuardTest.java` | 101 | tests for the unwired risk guard |
| `SizingScalesWithCapitalTest.java` | 89 | one sizing property |
| `java-bot` | 0 | empty tracked file, no content, no references |
| **total** | **13,628** | |

## What was removed, and why

**All of it.** Not as a judgement on the code — much of it was careful, and its comments record real
incidents — but because two of the brief's hard frames make it unkeepable on this branch:

1. **Frame 1 (testnet only, and a test that fails if a production URL appears in the code).**
   `SignalSender` hardcoded `https://fapi.binance.com` in fifteen places and
   `wss://fstream.binance.com` in two. `BinanceTradeExecutor` selected its endpoint from
   `BINANCE_USE_TESTNET`, **defaulting to `0` — the real exchange**. An unset variable on a fresh
   deployment selected real money. With that code present, the mandated test cannot pass, so the
   definition of done cannot be met. The brief resolves this explicitly: *"конфликт любого требования
   с рамками решается в пользу рамок"*.

2. **Frame 2 (no trading logic; the only entry is `SignalSource` with exactly two implementations).**
   `TradingCore` is an indicator library, `DecisionEngineMerged` is a signal generator, and
   `SignalSender` is both plus a market-data client. About 8,000 of the 13,628 lines are strategy —
   the category the frame excludes outright, and the category this project has already established
   has no edge (92 candidates, all failing pre-registered out-of-sample, walk-forward and
   null-model gates).

The rest followed from those two. `BotMain` wired only the deleted modules; `BinanceTradeExecutor`
was inseparable from its endpoint selection; `SupabaseSignalBridge` drove that executor;
`TelegramBotSender` existed to narrate the deleted pipeline.

### Safety code was not deleted — it was carried across

The brief exempts safety code from removal, and `RiskGuard` was the only module that qualified. Its
ideas are all present in `com.bot.risk`, several of them strengthened. Its own header said it best:
*"canTrade(), recordTradeOpened(), recordTradeClosed() НЕ ВЫЗЫВАЮТСЯ НИОТКУДА… ни одно ограничение
ниже не применяется ни к одной сделке."* It was 741 lines of correct risk logic that constrained
nothing, because nothing called it.

| carried from `RiskGuard` | now lives in | changed how |
|---|---|---|
| fail-closed on unreadable inputs | `RiskEngine`, `Preconditions` | unchanged in spirit — an unreadable balance still refuses rather than reading as zero |
| daily loss limit on the UTC day | `DailyLossKillSwitch` | now **latches** for the rest of the day instead of being re-evaluated per call |
| realised + open-drawdown accounting, with winners unable to mask losses | `DailyLossKillSwitch` | unchanged; it was already right |
| aggregate exposure as a share of balance, not a position count | `RiskConfig`, `ExposureBook` | now split **separately for long and short**, never netted |
| one position per symbol | `ExposureBook`, `RiskEngine` | unchanged |
| state surviving a restart | `DailyLossKillSwitch.seedRealizedPnl` | re-seeded from the **exchange's income ledger** instead of a local CSV — a crashed process cannot corrupt the exchange's ledger |
| `Decision(allowed, reason, hint)` | `RiskDecision` sealed + `RejectReason` enum | refusals are now enumerable, so "everything is being refused for one reason" is visible |

`SupabaseSignalBridge`'s queue contract was preserved rather than reinvented: `SupabaseQueueSource`
reads the same `public.bot_orders` table with the same conditional-PATCH claim, so an operator's
existing rows still work.

Telegram alerting was kept in spirit and dropped as a dependency: `AlertSink.Telegram` is one
env-configured HTTPS call, so the `org.telegram:telegrambots` library left the build along with the
408-line sender.

### Also removed

- `java-bot` — a tracked, empty, unreferenced file.
- `src/main/java/com/bot/data/calibrator.csv` — runtime state written by the deleted `BotMain` into
  the source tree.
- Build dependencies `org.telegram:telegrambots`, `telegrambots-meta` and `org.slf4j:slf4j-simple`,
  all now unused. `org.json` is the only runtime dependency left.

### One fix outside the brief's scope, made anyway

`local.env` carries a Supabase service key. Its own header says *"НЕ КОММИТИТЬ — файл в
.gitignore"* — but it was **not** in `.gitignore`. It sat untracked-but-committable since
2026-07-28; a single `git add -A` would have published a live service key. It is now ignored, along
with `*.env`. Nothing was committed, so no key rotation is required — but check `git log --all -p --
local.env` if you want that confirmed independently.

## What replaced it

| | files | lines |
|---|---:|---:|
| `src/main/java/com/bot/**` | 42 | 5,542 |
| `src/test/java/com/bot/**` | 24 | 3,160 |

13,628 lines removed, 8,702 added, and the test directory went from 190 lines covering one unwired
class to 156 tests covering the gate, the execution protocol and the frames themselves.

## Not touched

- `sql/paper_signals.sql` — the pre-registration schema. Unrelated to this layer and still correct.
- `AUDIT_2026-07-28_claims.md` — the previous audit. Historical record.
- `analysis/`, `archive/` — offline data, already git-ignored.
- `main` — untouched, as the brief requires. Review before merging.
