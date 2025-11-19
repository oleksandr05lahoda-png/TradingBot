import random
import matplotlib.pyplot as plt
import os

coins = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

def analyze_coin(coin):
    signal = random.choice(['LONG', 'SHORT'])
    confidence = round(random.uniform(0.5, 1.0), 2)
    # Генерируем пример графика
    prices = [random.uniform(100, 200) for _ in range(20)]
    plt.figure()
    plt.plot(prices, marker='o')
    plt.title(f"{coin} Signal: {signal}")
    plt.savefig(f"{coin}_chart.png")
    plt.close()
    return signal, confidence

if __name__ == "__main__":
    for coin in coins:
        signal, conf = analyze_coin(coin)
        print(f"{coin},{signal},{conf}")
