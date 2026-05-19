import json
from pybit.unified_trading import WebSocket
from kafka import KafkaProducer

print("DEBUG: Producer started, connecting to Bybit...")
ws = WebSocket(
    testnet=False,
    channel_type="spot"
)

producer = KafkaProducer(
    bootstrap_servers='host.docker.internal:9092',
    security_protocol='PLAINTEXT',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def handle_trade(message):
    """
    Bybit присылает словарь с ключом 'data', где лежит список сделок.
    Мы обрабатываем каждую сделку из этого списка.
    """
    try:
        trades = message.get('data', [])
        if not isinstance(trades, list):
            trades = [trades]  # на случай, если придёт одиночная сделка

        for trade in trades:
            event = {
                "exchange": "bybit",
                "symbol": trade['s'],
                "price": float(trade['p']),
                "quantity": float(trade['v']),
                "time": trade['T']
            }
            producer.send('market.trades', value=event)
            print(f"Bybit Sent: {event['symbol']} @ {event['price']}")

    except Exception as e:
        print(f"Bybit Error: {e}")

ws.trade_stream(
    symbol="BTCUSDT",
    callback=handle_trade
)

while True:
    pass