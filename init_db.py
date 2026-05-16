import psycopg2

# Подключаемся к PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='admin',
    password='admin123',
    database='analytics'
)
cursor = conn.cursor()

# Создаём таблицу для хранения событий кликов
cursor.execute("""
    CREATE TABLE IF NOT EXISTS click_events (
        id SERIAL PRIMARY KEY,
        user_id INTEGER,
        product_id INTEGER,
        action VARCHAR(50),
        price NUMERIC(10, 2),
        event_timestamp TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW()
    );
""")

conn.commit()
cursor.close()
conn.close()

print("Таблица click_events создана успешно!")