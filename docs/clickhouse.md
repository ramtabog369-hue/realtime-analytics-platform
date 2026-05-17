# ClickHouse – быстрая аналитическая БД

## Особенности
- Колоночное хранение → высокая скорость аналитических запросов.
- Движки таблиц: **MergeTree** (основной), ReplacingMergeTree, SummingMergeTree.
- Поддерживает SQL, включая оконные функции.

## Основные команды
\`\`\`sql
CREATE TABLE events (
    event_time DateTime,
    product_id Int32,
    action String,
    price Float32
) ENGINE = MergeTree()
ORDER BY (event_time, product_id);

SELECT product_id, count() FROM events GROUP BY product_id ORDER BY count() DESC LIMIT 10;
\`\`\`

## Ссылки
- [ClickHouse Documentation](https://clickhouse.com/docs)