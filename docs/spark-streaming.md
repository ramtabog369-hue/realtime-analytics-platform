# Spark Structured Streaming – шпаргалка

## Основные понятия
- **Structured Streaming** – надстройка над Spark SQL для потоковой обработки.
- **DataFrame** – основная абстракция (как таблица).
- **Источники (source)**: Kafka, файлы, сокеты.
- **Приёмники (sink)**: консоль, файлы, Kafka, Delta Lake, JDBC (ClickHouse, PostgreSQL).

## Ключевые моменты
- Оконные агрегации требуют **watermark** (`withWatermark`) для ограничения состояния.
- Режимы вывода: `append`, `update`, `complete`.
- Checkpointing обязателен для отказоустойчивости.

## Запуск в Docker
\`\`\`bash
docker exec -it spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 script.py
\`\`\`

## Ссылки
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)