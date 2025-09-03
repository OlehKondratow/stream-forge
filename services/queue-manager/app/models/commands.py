"""
Pydantic модели для валидации входящих команд и конфигураций.
"""
from typing import List, Literal, Optional
from pydantic import BaseModel, Field, constr, validator

# Определение поддерживаемых типов данных и сервисов для валидации
SUPPORTED_TARGETS = Literal[
    "loader-producer",
    "arango-connector",
    "graph-builder",
    "gnn-trainer",
    "visualizer",
    "loader-ws",
    "arango-orderbook",
]

SUPPORTED_TYPE_PREFIXES = (
    "api_candles",
    "api_trades",
    "ws_candles",
    "ws_trades",
    "ws_depth",
    "ws_orderbook", # <-- Добавляем эту строку
    "gnn_graph",
    "gnn_train",
    "realtime_graph",
    "realtime_gnn_infer",
    "graph_metrics_stream",
)


class MicroserviceConfig(BaseModel):
    """
    Конфигурация для одного микросервиса внутри конвейера.
    """
    target: SUPPORTED_TARGETS = Field(..., description="Целевой микросервис для запуска.")
    type: str = Field(..., description="Тип задачи или данных (например, 'api_candles_5m').")
    image: Optional[str] = Field(None, description="Docker-образ для запуска. Если не указан, используется образ по умолчанию.")
    
    # Поля, которые могут быть переопределены для конкретного микросервиса
    collection_name: Optional[str] = Field(None, description="Имя коллекции в ArangoDB.")
    collection_inputs: Optional[List[str]] = Field(None, description="Список входных коллекций (для graph-builder).")
    collection_output: Optional[str] = Field(None, description="Выходная коллекция (для graph-builder).")
    model_output: Optional[str] = Field(None, description="Имя выходной модели (для gnn-trainer).")
    graph_collection: Optional[str] = Field(None, description="Коллекция с графом (для gnn-trainer).")
    inference_interval: Optional[str] = Field(None, description="Интервал инференса (для gnn-trainer).")
    source: Optional[str] = Field(None, description="Источник данных (для visualizer).")
    interval: Optional[str] = Field(None, description="Интервал свечей (например, '1m', '5m', '1h', '1d').")

    @validator("type")
    def validate_type(cls, v):
        if not v.startswith(SUPPORTED_TYPE_PREFIXES):
            raise ValueError(f"Неподдерживаемый тип: {v}. Должен начинаться с одного из: {SUPPORTED_TYPE_PREFIXES}")
        return v

    class Config:
        extra = "forbid"


class QueueStartRequest(BaseModel):
    """
    Модель запроса на запуск нового конвейера (очереди).
    """
    symbol: constr(strip_whitespace=True, to_upper=True) = Field(..., description="Торговый символ (например, 'BTCUSDT').")
    time_range: Optional[str] = Field(None, description="Диапазон дат для исторических данных (например, '2024-06-01:2024-06-30').")
    microservices: List[MicroserviceConfig] = Field(..., description="Список микросервисов для запуска в рамках конвейера.")

    class Config:
        extra = "forbid"
        schema_extra = {
            "example": {
                "symbol": "BTCUSDT",
                "time_range": "2024-06-01:2024-06-30",
                "microservices": [
                    {"target": "loader-producer", "type": "api_candles_5m", "interval": "5m"},
                    {"target": "arango-connector", "type": "api_candles_5m"},
                ],
            }
        }

# --- Legacy and Test Models ---

class StartTestFlowCommand(BaseModel):
    symbol: str = Field(
        default="DUMMYUSDT",
        description="Символ для тестового прогона. Будет приведен к верхнему регистру."
    )
    time_range: str = Field(
        default="2024-01-01:2024-01-02",
        description="Небольшой диапазон дат для теста. Формат: YYYY-MM-DD:YYYY-MM-DD"
    )
    flow_name: str = Field(
        default="default_trade_flow",
        description="Название тестового прогона из файла test_flows.yaml"
    )

class StartQueueCommand(BaseModel):
    symbol: str
    type: str
    time_range: str
    target: str
    kafka_topic: str
    telemetry_id: str
    image: str
    interval: Optional[str] = None
    collection_name: Optional[str] = None