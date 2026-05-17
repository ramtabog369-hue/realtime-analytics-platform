# Docker Compose – шпаргалка

## Основные команды
- `docker compose up -d` – запустить сервисы в фоне.
- `docker compose down` – остановить и удалить контейнеры.
- `docker compose logs -f kafka` – логи конкретного сервиса.
- `docker compose restart` – перезапустить сервисы.

## Структура docker-compose.yml
- `services` – список контейнеров.
- `ports` – проброс портов (хост:контейнер).
- `environment` – переменные окружения.
- `volumes` – постоянные данные.

## Советы
- Всегда используйте `depends_on` для порядка запуска.
- Не храните пароли в открытом виде (используйте `.env` и Vault).