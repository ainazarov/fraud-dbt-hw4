# Kafka → ClickHouse

Домашнее задание №2: загрузка транзакций из CSV через Kafka в ClickHouse, поиск категории наибольшей транзакции по каждому штату и оптимизация хранения.

## Реализация

1. [scripts/produce.py](scripts/produce.py) — загрузка CSV в топик Kafka.
2. [sql/001_schema.sql](sql/001_schema.sql) — DDL таблиц ClickHouse и materialized views для приёма данных из Kafka.
3. [sql/004_max_by_state.sql](sql/004_max_by_state.sql) — категория максимальной отдельной транзакции в каждом штате. Готовые CSV: [train](reports/max_by_state_train.csv), [test](reports/max_by_state_test.csv).
4. Оптимизация: `LowCardinality`, сжатие `ZSTD`, сортировка детальной таблицы и `AggregatingMergeTree` с предварительным вычислением `argMaxState`. Итоговый запрос использует `argMaxMerge`.

Сборка и зависимости: [docker-compose.yml](docker-compose.yml), [Dockerfile](Dockerfile), [requirements.txt](requirements.txt).

## Запуск и проверка

Нужны Docker и Docker Compose 2.20+. Команды выполняются в Bash; на Windows — в WSL с доступом к Docker.

```bash
git clone https://github.com/ainazarov/fraud-analytics-hw2.git
cd fraud-analytics-hw2
bash scripts/demo.sh
```

Скрипт собирает образы, запускает контейнеры, отправляет 24 демонстрационные транзакции и проверяет результаты. CSV и протоколы проверки сохраняются в `results/`.

## Полный датасет

Скачайте `train.csv` и `test.csv` из [Kaggle](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394), положите их в `data/` и выполните из корня проекта:

```bash
bash scripts/full_run.sh
```

Скрипт загрузит оба файла, сравнит SQL-результаты с независимым расчётом по CSV и измерит скорость запросов. Результаты: `results/max_by_state_train.csv` и `results/max_by_state_test.csv`; train и test обрабатываются раздельно.

Полный импорт запускается один раз на текущих томах: повторная отправка добавит дубликаты. Остановка с сохранением данных: `docker compose down`.

## Результат оптимизации

Проверено 786 431 строка train и 262 144 строки test; получено по 51 результату для штатов и DC.

| Данные | Исходный запрос, мс | Запрос по агрегированной таблице, мс |
|---|---:|---:|
| train | 51 | 1 |
| test | 21 | 1 |

Указаны медианы 15 запусков с прогретым кэшем. Объём сжатых данных детальной таблицы уменьшился на 51,46%. Ускорение запроса обеспечила предагрегация. Измерения и условия проверки — в [reports/RESULTS.md](reports/RESULTS.md).
