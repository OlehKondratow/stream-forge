# `indicator-calculator`

<details>
<summary><strong>English</strong></summary>

A microservice in the **StreamForge** ecosystem designed for real-time calculation of technical indicators and preparation of features for ML models.

---

## 1. Purpose

`indicator-calculator` is a powerful data processing engine that performs the following tasks:

1.  **Consumes** real-time trade and order book data from Apache Kafka topics.
2.  **Synchronizes** multiple data streams based on event timestamps to ensure data integrity, even if topics are not perfectly aligned.
3.  **Aggregates** raw data into time-based candles (e.g., 1-minute, 5-minute).
4.  **Calculates** a wide range of technical indicators (RSI, MACD, etc.) using the `pandas-ta` library.
5.  **Computes** volume profiles for each candle, showing the distribution of traded volume across different price levels.
6.  **Generates** feature sets specifically for Reinforcement Learning (RL), including historical lookbacks and z-score normalized values.
7.  **Saves** the enriched data — containing the raw candle, visualization-ready indicators, volume profiles, and RL-ready state vectors — to an ArangoDB database.

This service is a stateless worker and is intended to run as a long-running **Kubernetes Deployment**.

---

## 2. Environment Variables

The service is fully configured through environment variables.

| Variable                      | Description                                                                                                                               | Example                                                                                                                                                           |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`QUEUE_ID`**                | Unique identifier for the workflow or instance. Used in logging and for the consumer group ID.                                            | `indicator-calculator-pixel`                                                                                                                                      |
| **`SYMBOL`**                  | Symbol or data identifier being processed.                                                                                                | `PIXELUSDT`                                                                                                                                                       |
| **`LOG_LEVEL`**               | The logging level for the service.                                                                                                        | `INFO`                                                                                                                                                            |
| `KAFKA_BOOTSTRAP_SERVERS`     | Kafka broker addresses.                                                                                                                   | `stf-kafka-bootstrap:9092`                                                                                                                                        |
| `KAFKA_TOPIC_TRADES`          | The Kafka topic for trade data.                                                                                                           | `pixelusdt-trades`                                                                                                                                                |
| `KAFKA_TOPIC_ORDERBOOK`       | The Kafka topic for order book data.                                                                                                      | `pixelusdt-orderbook`                                                                                                                                             |
| `KAFKA_USER_CONSUMER`         | Username for Kafka authentication.                                                                                                        | `user-consumer`                                                                                                                                                   |
| `KAFKA_PASSWORD_CONSUMER`     | Password for Kafka authentication.                                                                                                        | `your_kafka_password`                                                                                                                                             |
| `CA_PATH`                     | Path to the CA certificate for the Kafka TLS connection.                                                                                  | `/certs/ca.crt`                                                                                                                                                   |
| `ARANGO_URL`                  | URL for connecting to ArangoDB.                                                                                                           | `http://stf-arango-single:8529`                                                                                                                                   |
| `ARANGO_DB`                   | ArangoDB database name.                                                                                                                   | `stream_forge`                                                                                                                                                    |
| `ARANGO_USER`                 | ArangoDB username.                                                                                                                        | `root`                                                                                                                                                            |
| `ARANGO_PASSWORD`             | ArangoDB password.                                                                                                                        | `your_arango_password`                                                                                                                                            |
| `DB_COLLECTION`               | The ArangoDB collection where the enriched data will be stored.                                                                           | `technical_indicators_stream`                                                                                                                                     |
| `CANDLE_INTERVAL`             | The time interval for candle aggregation.                                                                                                 | `1m`                                                                                                                                                              |
| `CANDLE_WINDOW_SIZE`          | The number of candles to keep in memory for indicator calculation. Should be large enough for the longest indicator period.                 | `40`                                                                                                                                                              |
| `VOLUME_PROFILE_PRICE_STEP`   | The price step for aggregating volume in the volume profile. A smaller value means higher granularity.                                      | `0.00001`                                                                                                                                                         |
| `RL_LOOKBACK_PERIOD`          | The lookback period (number of candles) for generating historical features for the RL state.                                                | `20`                                                                                                                                                              |
| `INDICATORS_CONFIG`           | A JSON string defining which indicators to calculate and how. See the configuration example below.                                        | `'[{"name": "rsi", "enabled": true, "params": {"length": 14}}, {"name": "macd", "enabled": true, "normalize": true}]'`                                           |
| `TELEMETRY_TOPIC`             | Kafka topic for sending telemetry events.                                                                                                 | `telemetry`                                                                                                                                                       |
| `CONTROL_TOPIC`               | Kafka topic for receiving control commands (e.g., `stop`).                                                                                | `control`                                                                                                                                                         |

---

## 3. Indicator Configuration

The `INDICATORS_CONFIG` variable allows for flexible configuration of the indicators to be calculated. It is a JSON array of objects, where each object represents an indicator.

**Example `INDICATORS_CONFIG` value:**
```json
[
  { 
    "name": "rsi", 
    "enabled": true, 
    "params": { "length": 14 } 
  },
  { 
    "name": "macd", 
    "enabled": true, 
    "params": { "fast": 12, "slow": 26, "signal": 9 },
    "normalize": true 
  },
  { 
    "name": "adx", 
    "enabled": true, 
    "params": { "length": 14 },
    "normalize": true 
  }
]
```
*   `name`: The name of the indicator as used in the `pandas-ta` library.
*   `enabled`: A boolean to easily turn the calculation on or off.
*   `params`: An object containing the parameters for the indicator function.
*   `normalize`: (Optional) A boolean. If `true`, the historical data for this indicator in the `rl_state` will be z-score normalized. This is recommended for unbounded indicators like MACD or ADX.

---

## 4. Output Data Structure

The service saves a JSON document to ArangoDB for each finalized candle. The structure is designed to be useful for both visualization and machine learning.

**Example Document:**
```json
{
  "_key": "PIXELUSDT_1757063460000",
  "symbol": "PIXELUSDT",
  "timestamp": 1757063460000,
  "candle": {
    "open": 0.02901,
    "high": 0.02902,
    "low": 0.02901,
    "close": 0.02902,
    "volume": 8703,
    "quote_volume": 252.546139
  },
  "indicators": {
    "rsi_14": 56.24,
    "MACD_12_26_9": -0.0000064,
    "MACDh_12_26_9": 0.0000060,
    "MACDs_12_26_9": -0.0000124
  },
  "rl_state": {
    "open_norm_hist": [ ... ],
    "high_norm_hist": [ ... ],
    "low_norm_hist": [ ... ],
    "close_norm_hist": [ ... ],
    "volume_norm_hist": [ ... ],
    "rsi_14_hist": [ ... ],
    "MACD_12_26_9_hist": [ ... ]
  },
  "volume_profile": {
    "0.02901": 1492.1,
    "0.02902": 7210.9
  },
  "metadata": {
    "source": "kafka_trades_tob",
    "processed_at": "2025-09-05T09:12:19.354593+00:00"
  }
}
```

</details>

<details>
<summary><strong>Русский</strong></summary>

Микросервис в экосистеме **StreamForge**, предназначенный для расчета технических индикаторов и подготовки признаков для ML-моделей в реальном времени.

---

## 1. Назначение

`indicator-calculator` — это мощный движок обработки данных, который выполняет следующие задачи:

1.  **Потребляет** данные о сделках и биржевых стаканах в реальном времени из топиков Apache Kafka.
2.  **Синхронизирует** несколько потоков данных на основе временных меток событий для обеспечения целостности данных, даже если топики не идеально согласованы.
3.  **Агрегирует** сырые данные во временные свечи (например, 1-минутные, 5-минутные).
4.  **Рассчитывает** широкий спектр технических индикаторов (RSI, MACD и т.д.), используя библиотеку `pandas-ta`.
5.  **Вычисляет** профили объема для каждой свечи, показывая распределение проторгованного объема по ценовым уровням.
6.  **Генерирует** наборы признаков специально для Обучения с подкреплением (RL), включая исторические данные и нормализованные значения (z-score).
7.  **Сохраняет** обогащенные данные — содержащие сырую свечу, готовые для визуализации индикаторы, профили объема и готовые для RL векторы состояний — в базу данных ArangoDB.

Этот сервис является воркером без состояния и предназначен для запуска в качестве долгоживущего **Kubernetes Deployment**.

---

## 2. Переменные окружения

Сервис полностью настраивается через переменные окружения.

| Переменная                    | Описание                                                                                                                                  | Пример                                                                                                                                                            |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`QUEUE_ID`**                | Уникальный идентификатор рабочего процесса или экземпляра. Используется в логах и для ID группы потребителя.                               | `indicator-calculator-pixel`                                                                                                                                      |
| **`SYMBOL`**                  | Символ или идентификатор обрабатываемых данных.                                                                                           | `PIXELUSDT`                                                                                                                                                       |
| **`LOG_LEVEL`**               | Уровень логирования для сервиса.                                                                                                          | `INFO`                                                                                                                                                            |
| `KAFKA_BOOTSTRAP_SERVERS`     | Адреса брокеров Kafka.                                                                                                                    | `stf-kafka-bootstrap:9092`                                                                                                                                        |
| `KAFKA_TOPIC_TRADES`          | Топик Kafka с данными о сделках.                                                                                                          | `pixelusdt-trades`                                                                                                                                                |
| `KAFKA_TOPIC_ORDERBOOK`       | Топик Kafka с данными о биржевом стакане.                                                                                                 | `pixelusdt-orderbook`                                                                                                                                             |
| `KAFKA_USER_CONSUMER`         | Имя пользователя для аутентификации в Kafka.                                                                                              | `user-consumer`                                                                                                                                                   |
| `KAFKA_PASSWORD_CONSUMER`     | Пароль для аутентификации в Kafka.                                                                                                        | `your_kafka_password`                                                                                                                                             |
| `CA_PATH`                     | Путь к CA-сертификату для TLS-соединения с Kafka.                                                                                         | `/certs/ca.crt`                                                                                                                                                   |
| `ARANGO_URL`                  | URL для подключения к ArangoDB.                                                                                                           | `http://stf-arango-single:8529`                                                                                                                                   |
| `ARANGO_DB`                   | Имя базы данных в ArangoDB.                                                                                                               | `stream_forge`                                                                                                                                                    |
| `ARANGO_USER`                 | Имя пользователя ArangoDB.                                                                                                                | `root`                                                                                                                                                            |
| `ARANGO_PASSWORD`             | Пароль пользователя ArangoDB.                                                                                                             | `your_arango_password`                                                                                                                                            |
| `DB_COLLECTION`               | Коллекция в ArangoDB, куда будут сохраняться обогащенные данные.                                                                          | `technical_indicators_stream`                                                                                                                                     |
| `CANDLE_INTERVAL`             | Временной интервал для агрегации свечей.                                                                                                  | `1m`                                                                                                                                                              |
| `CANDLE_WINDOW_SIZE`          | Количество свечей, хранимых в памяти для расчета индикаторов. Должно быть достаточным для самого длинного периода индикатора.              | `40`                                                                                                                                                              |
| `VOLUME_PROFILE_PRICE_STEP`   | Ценовой шаг для агрегации объема в профиле объема. Меньшее значение означает более высокую гранулярность.                                   | `0.00001`                                                                                                                                                         |
| `RL_LOOKBACK_PERIOD`          | Период ретроспективы (количество свечей) для генерации исторических признаков для состояния RL.                                             | `20`                                                                                                                                                              |
| `INDICATORS_CONFIG`           | JSON-строка, определяющая, какие индикаторы рассчитывать и как. См. пример конфигурации ниже.                                              | `'[{"name": "rsi", "enabled": true, "params": {"length": 14}}, {"name": "macd", "enabled": true, "normalize": true}]'`                                           |
| `TELEMETRY_TOPIC`             | Топик Kafka для отправки событий телеметрии.                                                                                              | `telemetry`                                                                                                                                                       |
| `CONTROL_TOPIC`               | Топик Kafka для получения управляющих команд (например, `stop`).                                                                          | `control`                                                                                                                                                         |

---

## 3. Конфигурация индикаторов

Переменная `INDICATORS_CONFIG` позволяет гибко настраивать рассчитываемые индикаторы. Это JSON-массив объектов, где каждый объект представляет один индикатор.

**Пример значения `INDICATORS_CONFIG`:**
```json
[
  { 
    "name": "rsi", 
    "enabled": true, 
    "params": { "length": 14 } 
  },
  { 
    "name": "macd", 
    "enabled": true, 
    "params": { "fast": 12, "slow": 26, "signal": 9 },
    "normalize": true 
  },
  { 
    "name": "adx", 
    "enabled": true, 
    "params": { "length": 14 },
    "normalize": true 
  }
]
```
*   `name`: Название индикатора, как оно используется в библиотеке `pandas-ta`.
*   `enabled`: Булево значение для быстрого включения/отключения расчета.
*   `params`: Объект, содержащий параметры для функции индикатора.
*   `normalize`: (Опционально) Булево значение. Если `true`, исторические данные для этого индикатора в `rl_state` будут нормализованы с помощью z-score. Рекомендуется для неограниченных индикаторов, таких как MACD или ADX.

---

## 4. Структура выходных данных

Сервис сохраняет JSON-документ в ArangoDB для каждой завершенной свечи. Структура разработана так, чтобы быть полезной как для визуализации, так и для машинного обучения.

**Пример документа:**
```json
{
  "_key": "PIXELUSDT_1757063460000",
  "symbol": "PIXELUSDT",
  "timestamp": 1757063460000,
  "candle": {
    "open": 0.02901,
    "high": 0.02902,
    "low": 0.02901,
    "close": 0.02902,
    "volume": 8703,
    "quote_volume": 252.546139
  },
  "indicators": {
    "rsi_14": 56.24,
    "MACD_12_26_9": -0.0000064,
    "MACDh_12_26_9": 0.0000060,
    "MACDs_12_26_9": -0.0000124
  },
  "rl_state": {
    "open_norm_hist": [ ... ],
    "high_norm_hist": [ ... ],
    "low_norm_hist": [ ... ],
    "close_norm_hist": [ ... ],
    "volume_norm_hist": [ ... ],
    "rsi_14_hist": [ ... ],
    "MACD_12_26_9_hist": [ ... ]
  },
  "volume_profile": {
    "0.02901": 1492.1,
    "0.02902": 7210.9
  },
  "metadata": {
    "source": "kafka_trades_tob",
    "processed_at": "2025-09-05T09:12:19.354593+00:00"
  }
}
```

</details>

<details>
<summary><strong>Polski</strong></summary>

Mikrousługa w ekosystemie **StreamForge**, przeznaczona do obliczania wskaźników technicznych w czasie rzeczywistym i przygotowywania cech dla modeli ML.

---

## 1. Cel

`indicator-calculator` to potężny silnik przetwarzania danych, który wykonuje następujące zadania:

1.  **Pobiera** w czasie rzeczywistym dane o transakcjach i zleceniach z tematów Apache Kafka.
2.  **Synchronizuje** wiele strumieni danych na podstawie sygnatur czasowych zdarzeń, aby zapewnić integralność danych, nawet jeśli tematy nie są idealnie zsynchronizowane.
3.  **Agreguje** surowe dane w świece czasowe (np. 1-minutowe, 5-minutowe).
4.  **Oblicza** szeroki zakres wskaźników technicznych (RSI, MACD itp.), korzystając z biblioteki `pandas-ta`.
5.  **Oblicza** profile wolumenu для каждой свечи, pokazując rozkład wolumenu transakcji na różnych poziomach cenowych.
6.  **Generuje** zestawy cech specjalnie dla uczenia przez wzmacnianie (RL), w tym dane historyczne i wartości znormalizowane (z-score).
7.  **Zapisuje** wzbogacone dane — zawierające surową świecę, wskaźniki gotowe do wizualizacji, profile wolumenu i wektory stanu gotowe do RL — do bazy danych ArangoDB.

Ta usługa jest bezstanowym workerem i jest przeznaczona do działania jako długo działający **Kubernetes Deployment**.

---

## 2. Zmienne środowiskowe

Usługa jest w pełni konfigurowana za pomocą zmiennych środowiskowych.

| Zmienna                       | Opis                                                                                                                                      | Przykład                                                                                                                                                          |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`QUEUE_ID`**                | Unikalny identyfikator przepływu pracy lub instancji. Używany w logach i jako ID grupy konsumenta.                                         | `indicator-calculator-pixel`                                                                                                                                      |
| **`SYMBOL`**                  | Symbol lub identyfikator przetwarzanych danych.                                                                                           | `PIXELUSDT`                                                                                                                                                       |
| **`LOG_LEVEL`**               | Poziom logowania dla usługi.                                                                                                              | `INFO`                                                                                                                                                            |
| `KAFKA_BOOTSTRAP_SERVERS`     | Adresy brokerów Kafka.                                                                                                                    | `stf-kafka-bootstrap:9092`                                                                                                                                        |
| `KAFKA_TOPIC_TRADES`          | Temat Kafka z danymi o transakcjach.                                                                                                      | `pixelusdt-trades`                                                                                                                                                |
| `KAFKA_TOPIC_ORDERBOOK`       | Temat Kafka z danymi o zleceniach.                                                                                                        | `pixelusdt-orderbook`                                                                                                                                             |
| `KAFKA_USER_CONSUMER`         | Nazwa użytkownika do uwierzytelniania w Kafka.                                                                                            | `user-consumer`                                                                                                                                                   |
| `KAFKA_PASSWORD_CONSUMER`     | Hasło do uwierzytelniania w Kafka.                                                                                                        | `your_kafka_password`                                                                                                                                             |
| `CA_PATH`                     | Ścieżka do certyfikatu CA dla połączenia TLS z Kafką.                                                                                     | `/certs/ca.crt`                                                                                                                                                   |
| `ARANGO_URL`                  | URL do połączenia z ArangoDB.                                                                                                             | `http://stf-arango-single:8529`                                                                                                                                   |
| `ARANGO_DB`                   | Nazwa bazy danych w ArangoDB.                                                                                                             | `stream_forge`                                                                                                                                                    |
| `ARANGO_USER`                 | Nazwa użytkownika ArangoDB.                                                                                                               | `root`                                                                                                                                                            |
| `ARANGO_PASSWORD`             | Hasło użytkownika ArangoDB.                                                                                                               | `your_arango_password`                                                                                                                                            |
| `DB_COLLECTION`               | Kolekcja w ArangoDB, w której będą przechowywane wzbogacone dane.                                                                         | `technical_indicators_stream`                                                                                                                                     |
| `CANDLE_INTERVAL`             | Interwał czasowy do agregacji świec.                                                                                                      | `1m`                                                                                                                                                              |
| `CANDLE_WINDOW_SIZE`          | Liczba świec przechowywanych w pamięci do obliczania wskaźników. Powinna być wystarczająco duża dla najdłuższego okresu wskaźnika.           | `40`                                                                                                                                                              |
| `VOLUME_PROFILE_PRICE_STEP`   | Krok cenowy do agregacji wolumenu w profilu wolumenu. Mniejsza wartość oznacza większą szczegółowość.                                        | `0.00001`                                                                                                                                                         |
| `RL_LOOKBACK_PERIOD`          | Okres historyczny (liczba świec) do generowania cech historycznych dla stanu RL.                                                          | `20`                                                                                                                                                              |
| `INDICATORS_CONFIG`           | Ciąg znaków JSON definiujący, które wskaźniki mają być obliczane i jak. Zobacz przykład konfiguracji poniżej.                               | `'[{"name": "rsi", "enabled": true, "params": {"length": 14}}, {"name": "macd", "enabled": true, "normalize": true}]'`                                           |
| `TELEMETRY_TOPIC`             | Temat Kafka do wysyłania zdarzeń telemetrycznych.                                                                                         | `telemetry`                                                                                                                                                       |
| `CONTROL_TOPIC`               | Temat Kafka do odbierania poleceń sterujących (np. `stop`).                                                                               | `control`                                                                                                                                                         |

---

## 3. Konfiguracja wskaźników

Zmienna `INDICATORS_CONFIG` pozwala na elastyczną konfigurację obliczanych wskaźników. Jest to tablica JSON obiektów, gdzie każdy obiekt reprezentuje jeden wskaźnik.

**Przykład wartości `INDICATORS_CONFIG`:**
```json
[
  { 
    "name": "rsi", 
    "enabled": true, 
    "params": { "length": 14 } 
  },
  { 
    "name": "macd", 
    "enabled": true, 
    "params": { "fast": 12, "slow": 26, "signal": 9 },
    "normalize": true 
  },
  { 
    "name": "adx", 
    "enabled": true, 
    "params": { "length": 14 },
    "normalize": true 
  }
]
```
*   `name`: Nazwa wskaźnika, używana w bibliotece `pandas-ta`.
*   `enabled`: Wartość logiczna do łatwego włączania/wyłączania obliczeń.
*   `params`: Obiekt zawierający parametry dla funkcji wskaźnika.
*   `normalize`: (Opcjonalnie) Wartość logiczna. Jeśli `true`, dane historyczne dla tego wskaźnika w `rl_state` zostaną znormalizowane za pomocą z-score. Zalecane dla wskaźników nieograniczonych, takich jak MACD czy ADX.

---

## 4. Struktura danych wyjściowych

Usługa zapisuje dokument JSON do ArangoDB dla każdej ukończonej świecy. Struktura została zaprojektowana tak, aby była użyteczna zarówno do wizualizacji, jak i do uczenia maszynowego.

**Przykład dokumentu:**
```json
{
  "_key": "PIXELUSDT_1757063460000",
  "symbol": "PIXELUSDT",
  "timestamp": 1757063460000,
  "candle": {
    "open": 0.02901,
    "high": 0.02902,
    "low": 0.02901,
    "close": 0.02902,
    "volume": 8703,
    "quote_volume": 252.546139
  },
  "indicators": {
    "rsi_14": 56.24,
    "MACD_12_26_9": -0.0000064,
    "MACDh_12_26_9": 0.0000060,
    "MACDs_12_26_9": -0.0000124
  },
  "rl_state": {
    "open_norm_hist": [ ... ],
    "high_norm_hist": [ ... ],
    "low_norm_hist": [ ... ],
    "close_norm_hist": [ ... ],
    "volume_norm_hist": [ ... ],
    "rsi_14_hist": [ ... ],
    "MACD_12_26_9_hist": [ ... ]
  },
  "volume_profile": {
    "0.02901": 1492.1,
    "0.02902": 7210.9
  },
  "metadata": {
    "source": "kafka_trades_tob",
    "processed_at": "2025-09-05T09:12:19.354593+00:00"
  }
}
```

</details>
