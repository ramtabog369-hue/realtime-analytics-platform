import redis
import psycopg2
import json
import time

# Подключаемся к Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Подключаемся к PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='admin',
    password='admin123',
    database='analytics'
)
cursor = conn.cursor()

print("Кэш-сервис запущен. Обновляю топ-5 товаров каждые 10 секунд...")
print("Нажми Ctrl+C чтобы остановить.\n")

try:
    while True:
        # Считаем топ-5 товаров по количеству событий
        cursor.execute("""
            SELECT product_id, COUNT(*) as event_count
            FROM click_events
            GROUP BY product_id
            ORDER BY event_count DESC
            LIMIT 5
        """)
        
        top_products = cursor.fetchall()
        
        # Формируем данные для Redis
        top_list = []
        for i, (product_id, count) in enumerate(top_products, 1):
            top_list.append({
                "rank": i,
                "product_id": product_id,
                "event_count": count
            })
        
        # Сохраняем в Redis (ключ "top5_products", храним 30 секунд)
        r.setex("top5_products", 30, json.dumps(top_list))
        
        print(f"Обновлён топ-5:")
        for item in top_list:
            print(f"  #{item['rank']} Товар {item['product_id']} — {item['event_count']} событий")
        print()
        
        time.sleep(10)  # Обновляем каждые 10 секунд

except KeyboardInterrupt:
    print("\nКэш-сервис остановлен.")
    cursor.close()
    conn.close()
    r.close()