import asyncio
import json
import ssl
from aiokafka import AIOKafkaConsumer
from arango import ArangoClient
from loguru import logger
from app import config
from app.telemetry import TelemetryProducer

async def run_consumer(stop_event: asyncio.Event, telemetry: TelemetryProducer):
    logger.info("Starting Kafka consumer for generic data...")

    # Kafka Consumer Setup
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.load_verify_locations(cafile=config.CA_PATH)

    consumer = AIOKafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=config.KAFKA_USER,
        sasl_plain_password=config.KAFKA_PASSWORD,
        ssl_context=ssl_context,
        group_id=f"arango-connector-consumer-{config.QUEUE_ID}", # Unique group ID
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    await consumer.start()
    logger.info(f"Kafka consumer connected to topic: {config.KAFKA_TOPIC}")

    # ArangoDB Client Setup
    client = ArangoClient(hosts=config.ARANGO_URL)
    db = client.db(
        config.ARANGO_DB,
        username=config.ARANGO_USER,
        password=config.ARANGO_PASSWORD,
    )

    # Ensure the collection exists
    if not db.has_collection(config.COLLECTION_NAME):
        logger.info(f"Collection '{config.COLLECTION_NAME}' not found, creating it.")
        db.create_collection(config.COLLECTION_NAME)
    else:
        logger.info(f"Collection '{config.COLLECTION_NAME}' already exists.")
        
    collection = db.collection(config.COLLECTION_NAME)
    logger.info(f"Connected to ArangoDB collection: {config.COLLECTION_NAME}")

    records_processed = 0
    try:
        await telemetry.send_status_update(status="loading", message="Consumer started, processing data.")
        async for msg in consumer:
            if stop_event.is_set():
                logger.info("Stop event received, exiting consumer loop.")
                break

            try:
                data = json.loads(msg.value.decode("utf-8"))
                
                # Perform UPSERT operation. Assume data might have a '_key' or we generate one.
                # If '_key' is not present, ArangoDB will generate one.
                result = collection.insert(data, overwrite=True)
                
                records_processed += 1
                if records_processed % 100 == 0: # Send telemetry update every 100 records
                    await telemetry.send_status_update(
                        status="loading",
                        message=f"Processed {records_processed} records.",
                        records_written=records_processed
                    )
                logger.debug(f"Processed record: {data.get('_key', 'N/A')}")

            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON from Kafka message: {msg.value}")
                await telemetry.send_status_update(status="error", message="JSON decode error", error_message=f"Failed to decode JSON: {msg.value}")
            except Exception as e:
                logger.error(f"Error processing data: {e}", exc_info=True)
                await telemetry.send_status_update(status="error", message="Data processing error", error_message=str(e))

    except asyncio.CancelledError:
        logger.info("Consumer task cancelled.")
        await telemetry.send_status_update(status="interrupted", message="Consumer task cancelled.")
    except Exception as e:
        logger.error(f"Fatal error in consumer: {e}", exc_info=True)
        await telemetry.send_status_update(status="error", message="Fatal consumer error", error_message=str(e))
    finally:
        await consumer.stop()
        logger.info(f"Kafka consumer stopped. Total records processed: {records_processed}")
        await telemetry.send_status_update(
            status="finished",
            message=f"Consumer finished. Total records written: {records_processed}",
            finished=True,
            records_written=records_processed
        )
