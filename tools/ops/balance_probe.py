"""Read-only account probe. Prints numbers only - never a key, never a secret."""
import hmac, hashlib, json, time, urllib.parse, urllib.request

env = {}
for line in open('/opt/tradingbot.env', encoding='utf-8', errors='replace'):
    line = line.strip()
    if not line or line.startswith('#') or '=' not in line:
        continue
    k, v = line.split('=', 1)
    env[k.strip()] = v.strip()

KEY = env['BINANCE_REAL_API_KEY']
SEC = env['BINANCE_REAL_API_SECRET'].encode()
BASE = 'https://fapi.binance.com'


def signed(path, **params):
    params['timestamp'] = int(time.time() * 1000)
    params['recvWindow'] = 20000
    q = urllib.parse.urlencode(params)
    sig = hmac.new(SEC, q.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(BASE + path + '?' + q + '&signature=' + sig,
                                 headers={'X-MBX-APIKEY': KEY})
    return json.loads(urllib.request.urlopen(req, timeout=25).read())


acct = signed('/fapi/v2/account')
wallet = float(acct['totalWalletBalance'])
unreal = float(acct['totalUnrealizedProfit'])
print('WALLET      %.4f' % wallet)
print('UNREALIZED  %+.4f' % unreal)
print('EQUITY      %.4f' % (wallet + unreal))
print('AVAIL       %.4f' % float(acct['availableBalance']))
print()

pos = [p for p in acct['positions'] if abs(float(p['positionAmt'])) > 0]
print('OPEN %d' % len(pos))
tot_notional = 0.0
for p in sorted(pos, key=lambda x: -abs(float(x['unrealizedProfit']))):
    amt = float(p['positionAmt'])
    entry = float(p['entryPrice'])
    up = float(p['unrealizedProfit'])
    notional = abs(float(p.get('notional') or amt * entry))
    tot_notional += notional
    print('  %-12s notional %7.2f  uPnL %+7.4f  (%+.2f%%)'
          % (p['symbol'], notional, up, 100 * up / notional if notional else 0))
print('  TOTAL NOTIONAL %.2f' % tot_notional)
print()

# Realized income, whole account life
inc, start, seen = [], 0, set()
while True:
    batch = signed('/fapi/v1/income', startTime=start or 1, limit=1000)
    fresh = [r for r in batch if (r['tranId'], r['time'], r['income']) not in seen]
    if not fresh:
        break
    for r in fresh:
        seen.add((r['tranId'], r['time'], r['income']))
    inc.extend(fresh)
    start = max(r['time'] for r in batch) + 1
    if len(batch) < 1000:
        break

from collections import defaultdict
by_type = defaultdict(float)
for r in inc:
    by_type[r['incomeType']] += float(r['income'])
print('INCOME BY TYPE (whole account life, %d rows)' % len(inc))
for k, v in sorted(by_type.items(), key=lambda x: -abs(x[1])):
    print('  %-20s %+10.4f' % (k, v))

PNL = ('REALIZED_PNL', 'COMMISSION', 'FUNDING_FEE')
by_day = defaultdict(float)
for r in inc:
    if r['incomeType'] in PNL:
        d = time.strftime('%Y-%m-%d', time.gmtime(r['time'] / 1000))
        by_day[d] += float(r['income'])
print()
print('REALIZED NET BY DAY (pnl+fee+funding)')
run = 0.0
for d in sorted(by_day):
    run += by_day[d]
    print('  %s  %+8.4f   cum %+8.4f' % (d, by_day[d], run))
