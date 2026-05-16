import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'clicks'

print("Продюсер запущен. Шлю сообщения в топик clicks...")
print("Нажми Ctrl+C чтобы остановить.\n")

try:
    while True:
        event = {
            "user_id": random.randint(1, 1000),
            "product_id": random.randint(1, 500),
            "action": random.choice(["view", "click", "add_to_cart", "purchase"]),
            "price": round(random.uniform(100, 5000), 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        future = producer.send(TOPIC, value=event)
        record_metadata = future.get(timeout=10)

        print(f"OK {event['action']} | user={event['user_id']} | product={event['product_id']} | price={event['price']} | partition={record_metadata.partition} offset={record_metadata.offset}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nПродюсер остановлен.")
    producer.close()