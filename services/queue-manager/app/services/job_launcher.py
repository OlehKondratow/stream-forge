"""
Сервис для запуска заданий (Jobs) в Kubernetes.
"""
import asyncio # New import
from loguru import logger
from kubernetes import client, config

from app.config import settings
from app.models.commands import MicroserviceConfig
from app.utils.naming import sanitize_name

def load_k8s_config():
    """Загружает конфигурацию Kubernetes (in-cluster или из kubeconfig)."""
    try:
        config.load_incluster_config()
        logger.debug("Загружена in-cluster конфигурация Kubernetes.")
    except config.ConfigException:
        config.load_kube_config()
        logger.debug("Загружена конфигурация Kubernetes из kubeconfig.")


async def launch_k8s_job(
    job_name: str,
    image: str,
    env_vars: dict,
    namespace: str,
    restart_policy: str = "Never",
    backoff_limit: int = 4,
):
    """Создает и запускает Kubernetes Job."""
    api_instance = client.BatchV1Api()

    container_env = [client.V1EnvVar(name=k, value=str(v)) for k, v in env_vars.items()]

    container = client.V1Container(
        name=job_name,
        image=image,
        env=container_env,
        image_pull_policy="Always",
    )

    pod_template = client.V1PodTemplateSpec(
        spec=client.V1PodSpec(restart_policy=restart_policy, containers=[container])
    )

    job_spec = client.V1JobSpec(
        template=pod_template,
        backoff_limit=backoff_limit,
        ttl_seconds_after_finished=3600,  # Job будет удален через час после завершения
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name, namespace=namespace),
        spec=job_spec,
    )

    try:
        api_response = await asyncio.to_thread(api_instance.create_namespaced_job, body=job, namespace=namespace)
        logger.info(f"✅ Задание Kubernetes '{job_name}' успешно создано.")
        return api_response
    except client.ApiException as e:
        logger.error(f"❌ Ошибка при создании задания Kubernetes '{job_name}': {e.body}")
        raise


async def launch_job(
    queue_id: str,
    short_id: str,
    symbol: str,
    time_range: str | None,
    kafka_topic: str,
    collection_name: str,
    service_config: MicroserviceConfig,
):
    """Подготавливает и запускает одно задание в Kubernetes для микросервиса."""
    job_name = f"{sanitize_name(queue_id)}-{sanitize_name(service_config.target)}".lower()
    telemetry_id = f"{service_config.target}__{short_id}"

    # Определение Docker-образа
    image_to_use = service_config.image
    if not image_to_use:
        image_map = {
            "loader-producer": settings.LOADER_IMAGE,
            "arango-connector": settings.CONSUMER_IMAGE,
            "gnn-trainer": settings.GNN_TRAINER_IMAGE,
            "visualizer": settings.VISUALIZER_IMAGE,
            "graph-builder": settings.GRAPH_BUILDER_IMAGE,
        }
        image_to_use = image_map.get(service_config.target)
        if not image_to_use:
            raise ValueError(f"Не удалось найти образ по умолчанию для '{service_config.target}'")

    # Формирование переменных окружения
    env_vars = {
        "QUEUE_ID": queue_id,
        "SYMBOL": symbol,
        "TYPE": service_config.type,
        "KAFKA_TOPIC": kafka_topic,
        "TELEMETRY_PRODUCER_ID": telemetry_id,
        "KAFKA_BOOTSTRAP_SERVERS": settings.KAFKA_BOOTSTRAP_SERVERS,
        "KAFKA_USER": settings.KAFKA_USER,
        "KAFKA_PASSWORD": settings.KAFKA_PASSWORD,
        "CA_PATH": settings.CA_PATH,
        "QUEUE_CONTROL_TOPIC": settings.QUEUE_CONTROL_TOPIC,
        "QUEUE_EVENTS_TOPIC": settings.QUEUE_EVENTS_TOPIC,
        "ARANGO_URL": settings.ARANGO_URL,
        "ARANGO_DB": settings.ARANGO_DB,
        "ARANGO_USER": settings.ARANGO_USER,
        "ARANGO_PASSWORD": settings.ARANGO_PASSWORD,
        # Переменные, специфичные для микросервиса
        "COLLECTION_NAME": service_config.collection_name or collection_name,
    }
    if time_range: env_vars["TIME_RANGE"] = time_range
    if service_config.interval: env_vars["INTERVAL"] = service_config.interval
    if service_config.collection_inputs: env_vars["COLLECTION_INPUTS"] = ",".join(service_config.collection_inputs)
    if service_config.collection_output: env_vars["COLLECTION_OUTPUT"] = service_config.collection_output
    if service_config.model_output: env_vars["MODEL_OUTPUT"] = service_config.model_output
    if service_config.graph_collection: env_vars["GRAPH_COLLECTION"] = service_config.graph_collection

    logger.info(f"Запуск задания '{job_name}' с образом '{image_to_use}'")

    await launch_k8s_job(
        job_name=job_name,
        image=image_to_use,
        env_vars=env_vars,
        namespace=settings.K8S_NAMESPACE,
    )

# Загрузка конфигурации при импорте модуля
load_k8s_config()