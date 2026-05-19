import json
from binance import ThreadedWebsocketManager
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='YOUR_BOOTSTRAP_SERVER',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-256',
    sasl_plain_username='YOUR_USERNAME',
    sasl_plain_password='YOUR_PASSWORD',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def handle_trade(msg):
    trade = {
        "symbol": msg['s'],
        "price": float(msg['p']),
        "quantity": float(msg['q']),
        "time": msg['T']
    }
    producer.send('market.trades', value=trade)
    print(f"Sent: {trade['symbol']} @ {trade['price']}")

twm = ThreadedWebsocketManager()
twm.start_aggtrade_socket(callback=handle_trade, symbol='BTCUSDT')
twm.join()
