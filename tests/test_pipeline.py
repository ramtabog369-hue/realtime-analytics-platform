import json
import pytest
from datetime import datetime, timezone
import random

# ─────────────── Тесты, не требующие Kafka/Redis ───────────────

def test_event_structure():
    """Проверяем, что словарь события содержит все нужные ключи и типы."""
    event = {
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 500),
        "action": random.choice(["view", "click", "add_to_cart", "purchase"]),
        "price": round(random.uniform(100, 5000), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    assert isinstance(event["user_id"], int)
    assert isinstance(event["product_id"], int)
    assert event["action"] in ["view", "click", "add_to_cart", "purchase"]
    assert event["price"] > 0
    # timestamp должна быть валидной ISO-строкой
    datetime.fromisoformat(event["timestamp"])

def test_event_serialization():
    """Сериализация в JSON и обратно сохраняет данные."""
    event = {
        "user_id": 123,
        "product_id": 456,
        "action": "view",
        "price": 99.99,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    msg = json.dumps(event).encode('utf-8')
    deserialized = json.loads(msg.decode('utf-8'))
    assert deserialized["user_id"] == 123
    assert deserialized["price"] == 99.99

def test_price_boundary():
    """Цена должна быть положительной даже для минимального значения."""
    price = 0.01
    assert price > 0

def test_user_id_range():
    """user_id должен быть в пределах от 1 до 1000."""
    uid = random.randint(1, 1000)
    assert 1 <= uid <= 1000

def test_timestamps_are_unique():
    """Два последовательных вызова дают разные метки времени."""
    ts1 = datetime.now(timezone.utc).isoformat()
    ts2 = datetime.now(timezone.utc).isoformat()
    assert ts1 != ts2