import requests
import matplotlib.pyplot as plt

coins = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

def get_prices(coin):
    url = f"https://api.binance.com/api/v3/klines?symbol={coin}&interval=1m&limit=20"
    data = requests.get(url).json()
    return [float(x[4]) for x in data]  # закрытые цены

def analyze_coin(coin):
    prices = get_prices(coin)
    signal = "LONG" if prices[-1] > prices[0] else "SHORT"
    confidence = round(abs(prices[-1] - prices[0]) / prices[0], 2)

    plt.figure()
    plt.plot(prices, marker='o')
    plt.title(f"{coin} Signal: {signal}")
    plt.savefig(f"{coin}_chart.png")
    plt.close()

    return f"{coin},{signal},{confidence}"

if __name__ == "__main__":
    for coin in coins:
        print(analyze_coin(coin))
