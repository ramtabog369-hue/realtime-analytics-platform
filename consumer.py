import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# Подключаемся к Kafka (читаем топик clicks)
consumer = KafkaConsumer(
    'clicks',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Читаем с самого начала
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Подключаемся к PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='admin',
    password='admin123',
    database='analytics'
)
cursor = conn.cursor()

print("Consumer запущен. Жду сообщения из Kafka...")
print("Нажми Ctrl+C чтобы остановить.\n")

try:
    for message in consumer:
        event = message.value

        # Вставляем событие в PostgreSQL
        cursor.execute("""
            INSERT INTO click_events (user_id, product_id, action, price, event_timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            event['user_id'],
            event['product_id'],
            event['action'],
            event['price'],
            datetime.fromisoformat(event['timestamp'])
        ))

        conn.commit()

        print(f"Сохранено в БД: {event['action']} | user={event['user_id']} | product={event['product_id']} | offset={message.offset}")

except KeyboardInterrupt:
    print("\nConsumer остановлен.")
    cursor.close()
    conn.close()
    consumer.close()