# CryptoFlow Terminal – Полный 40-дневный план до Middle Data Engineer

> **Старт:** 18.05.2026  
> **Цель:** Построить платформу потоковой аналитики для крипторынка (Bybit, Binance, TradingView) и подготовиться к собеседованиям на Middle Data Engineer.  
> **Стек:** Python, Java, C#, SQL, Kafka, Flink, Spark, ClickHouse, Kubernetes, Airflow, GraphQL, React, Docker, Terraform, Prometheus, Grafana.

---

## ⏱️ Общая структура

- **Неделя 1–2:** Инфраструктура + сбор данных (K8s, Kafka, Producer) ✅
- **Неделя 3–4:** Аналитика (Spark, Flink, ClickHouse, Airflow)
- **Неделя 5–6:** Бэкенд и визуализация (GraphQL, React, TradingView)
- **Неделя 7:** DevOps, мониторинг, безопасность
- **Неделя 8:** Enterprise-инструменты (Hive, SAP обзор), финальный проект
- **Неделя 9:** Карьера, LeetCode, mock-интервью

Каждый день: 5–7 часов в будни, 8–10 в выходные.

---

## Неделя 1: Фундамент (уже позади)

### День 1 (18.05) ✅
- **Код:** `upstash_producer.py` (Binance → Kafka)
- **Инфра:** Kind-кластер локально, `kubectl`
- **Git:** Репозиторий `realtime-analytics-platform`
- **Блог:** Первый пост о старте

### День 2 (19.05) ✅
- **Код:** `bybit_producer.py` (Bybit → Kafka, приоритет)
- **Инфра:** Docker-образ для продюсера, CronJob в K8s
- **Интеграция:** TradingView webhook-приёмник (Flask)
- **Git:** Коммит с манифестами

---

## Неделя 2: Kafka + Java Crash Course

### День 3 (20.05) – Strimzi Kafka в K8s
- Установить Strimzi Operator (`kubectl create -f ...`)
- Создать манифест `kafka-cluster.yaml`
- Дождаться подов `cryptoflow-kafka-kafka-0`, `cryptoflow-kafka-zookeeper-0`
- Переключить `bybit_producer.py` на локальный Kafka (`cryptoflow-kafka:9092`)
- **SQL:** Топ‑3 валютных пар по объёму в ClickHouse (уже есть)

### День 4 (21.05) – Java Crash Course (часть 1)
- Установить JDK 17 (Amazon Corretto), IntelliJ IDEA Community
- **Основы:** типы, var, циклы, классы, интерфейсы, `public static void main`
- **Практика:** Написать `HelloKafka.java` – простое приложение, которое выводит «Привет, Kafka!»
- **Git:** Закоммитить Java-проект в папку `java-basics`

### День 5 (22.05) – Java Crash Course (часть 2)
- **Коллекции:** `List`, `Map`, `Set`, `ArrayList`, `HashMap`
- **Многопоточность:** `Thread`, `Runnable`, `ExecutorService`
- **Stream API:** `filter`, `map`, `collect`
- **Практика:** Написать `KafkaProducerDemo.java` – отправляет тестовое сообщение в Kafka из Java

### День 6 (23.05) – Apache Flink – теория + первый Job
- **Что такое Flink:** JobManager, TaskManager, Watermarks, State
- **Установка Flink Kubernetes Operator**
- **Практика:** Написать `FlinkTradeCount.java` – читает из Kafka топика `market.trades`, считает количество сделок по символам, выводит в консоль
- **Деплой:** Запустить как Flink Application в K8s

### День 7 (24.05) – SQL + Bash/Git (закрепление)
- **SQL:** Оконные функции (`ROW_NUMBER`, `LAG`), расчёт VWAP
- **Bash:** Скрипт для проверки состояния подов (`check_pods.sh`)
- **Git:** Интерактивный rebase (`git rebase -i HEAD~3`)

---

## Неделя 3: Аналитика (Spark, Flink, ClickHouse, Airflow)

### День 8 (25.05) – Spark Streaming в K8s
- Установить Spark Operator
- Написать `spark_footprint.py` – расчёт футпринта (bid/ask volume per price level)
- Собрать Docker-образ для Spark Job
- Задеплоить как SparkApplication

### День 9 (26.05) – ClickHouse в K8s
- Установить ClickHouse Operator
- Создать таблицы: `footprint`, `candles_1m`, `candles_5m`
- Spark начинает писать агрегаты в ClickHouse

### День 10 (27.05) – Airflow в K8s
- Развернуть Airflow через Helm
- Написать DAG `cryptoflow_etl`:
  - Запускает Spark-джобу
  - Проверяет наличие данных
  - Отправляет уведомление в Telegram

### День 11 (28.05) – Flink stateful processing
- **Практика:** `FlinkFootprint.java` – аналог Spark-футпринта на Flink, с использованием `MapState` для хранения объёмов на уровне цен
- Сравнение Spark vs Flink: скорость, сложность, отказоустойчивость

### День 12 (29.05) – Интеграция с Binance (резервный продюсер)
- Написать `binance_producer.py` по аналогии с Bybit
- Задеплоить как отдельный CronJob
- Настроить мониторинг Kafka lag (Prometheus + Grafana)

### День 13 (30.05) – Kafka Streams (Java)
- **Практика:** Написать `KafkaStreamsTradeAggregator.java` – агрегирует сделки в 1‑минутные свечи и пишет обратно в Kafka (топик `candles`)
- Сравнить Kafka Streams с Flink/Spark

### День 14 (31.05) – SQL, Git, Bash (закрепление)
- **SQL:** JOIN между таблицами `footprint` и `candles`, поиск аномалий
- **Git:** `git bisect`, `git reflog`
- **Bash:** Скрипт для перезапуска всех сервисов

---

## Неделя 4: Бэкенд и визуализация

### День 15 (01.06) – GraphQL API (Ariadne + FastAPI)
- Эндпоинты: `candles(symbol, interval)`, `footprint(symbol)`, `orderbook(symbol)`
- Подключение к ClickHouse через `clickhouse-driver`

### День 16 (02.06) – GraphQL API – фильтрация и пагинация
- Фильтры по времени, объёму
- Курсорная пагинация
- Кэширование в Redis

### День 17 (03.06) – Тестирование API
- Unit-тесты (pytest)
- Интеграционные тесты с тестовой БД
- Нагрузочное тестирование (locust)

### День 18 (04.06) – React-дашборд (базовый)
- Инициализация React-проекта (Vite + TypeScript)
- Свечной график (Lightweight Charts)
- Подключение к GraphQL API через Apollo Client

### День 19 (05.06) – React – стакан и лента сделок
- Компонент OrderBook (таблица bid/ask)
- Компонент TradeList (бесконечная лента сделок)
- Автообновление по WebSocket

### День 20 (06.06) – Дебаг и оптимизация
- Проверка утечек памяти
- Оптимизация запросов в ClickHouse (материализованные представления)
- Code review

### День 21 (07.06) – Документация и блог
- Написать ADR (Architecture Decision Records)
- Обновить README
- Пост в Telegram: «CryptoFlow Terminal: от идеи до работающего дашборда за 21 день»

---

## Неделя 5: DevOps, мониторинг, Enterprise

### День 22 (08.06) – CI/CD (GitHub Actions)
- Пайплайн: тесты → сборка образов → деплой в Kind
- Автоматический прогон тестов при пуше

### День 23 (09.06) – Terraform для Hetzner Cloud
- Описать инфраструктуру: VPS, сеть, брандмауэр
- Развернуть кластер на удалённом сервере

### День 24 (10.06) – Мониторинг и алертинг
- Prometheus: метрики Spark, ClickHouse, Kafka
- Grafana: дашборд со здоровьем системы
- Alertmanager: уведомления в Telegram при падении Pod'ов

### День 25 (11.06) – Безопасность
- HashiCorp Vault для хранения секретов
- Настроить TLS для Kafka и ClickHouse

### День 26 (12.06) – Hive и Data Warehouse
- Установить Hive в Docker
- Создать внешние таблицы поверх данных в Delta Lake
- SQL-запросы через HiveQL

### День 27 (13.06) – SAP (теоретический обзор)
- Что такое SAP BW, SAP HANA
- Как подключаться через RFC, ODP
- **Резюме:** «Знаком с принципами интеграции с SAP как источником данных»

### День 28 (14.06) – Резервное копирование и восстановление
- Бэкап ClickHouse на S3 (MinIO)
- Восстановление из бэкапа

---

## Неделя 6: Карьера и финальный проект

### День 29 (15.06) – Резюме и LinkedIn
- Обновить резюме с проектом CryptoFlow
- Добавить ключевые слова: Kafka, Flink, Spark, Kubernetes, ClickHouse, Bybit API, Java, C#

### День 30 (16.06) – Mock-интервью
- Пройти 3 технических собеседования с другом или на платформе (Pramp, interviewing.io)
- Разбор типичных вопросов: «Как выбрать между Spark и Flink?», «Как обеспечить exactly-once в Kafka?»

### День 31 (17.06) – LeetCode на Java (5 задач)
- Решить 5 задач Easy/Medium на LeetCode или Codewars
- Запушить в отдельный репозиторий `leetcode-solutions`

### День 32 (18.06) – LeetCode на C# (3 задачи)
- Решить 3 задачи на C#, добавить в тот же репозиторий
- Сравнить синтаксис Java и C# на одинаковых задачах

### День 33 (19.06) – Доработка Flink-проекта
- Добавить windowed join (соединение сделок и справочника инструментов)
- Добавить stateful enrichment (обогащение сделок статическими данными)

### День 34 (20.06) – Финальная документация
- Инструкция по развёртыванию с нуля (`SETUP.md`)
- Видео-демо работы терминала (запись экрана)
- Статья «Как я построил CryptoFlow Terminal за 34 дня»

### День 35 (21.06) – Ретроспектива и отдых
- Подвести итоги, заполнить learning-log
- Опубликовать финальный пост в Telegram
- Взять выходной 😎

---

## 🔧 Оставшиеся 5 дней (бонусные)
Эти дни можно использовать для углубления в любую тему: C# API, дополнительные задачи LeetCode, подготовка к конкретной вакансии.

---

## 📊 График загрузки
- **5–7 часов в день** в будни
- **9–10 часов** по выходным
- Каждый вечер – запись в learning-log и пост в Telegram

---

## 🎯 Что ты будешь уметь через 40 дней
- Разворачивать Kubernetes-кластер и управлять им
- Настраивать Kafka (Strimzi), ClickHouse, Airflow, Flink, Spark в K8s
- Писать потоковые Job'ы на Python и Java
- Создавать GraphQL API и React-дашборды
- Настраивать CI/CD, мониторинг, алертинг
- Работать с реальными криптобиржами (Bybit, Binance)
- Проходить технические собеседования на Middle Data Engineer