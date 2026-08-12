import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer
from sqlalchemy.orm import Session

from app import ImportJob, engine

BASE_DIR = Path(__file__).resolve().parent
REQUEST_TOPIC = os.getenv("KAFKA_IMPORT_TOPIC", "turismo.importar-locais")
PLACE_TOPIC = os.getenv("KAFKA_PLACE_TOPIC", "turismo.processar-lugar")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("turismo-coordinator")


def update_job(job_id: str, **values) -> None:
    with Session(engine) as session:
        job = session.get(ImportJob, job_id)
        if job:
            for field, value in values.items():
                setattr(job, field, value)
            session.commit()


def distribute(job_id: str, producer: Producer) -> None:
    with Session(engine) as session:
        job = session.get(ImportJob, job_id)
        if not job:
            logger.warning("Job %s não existe; solicitação ignorada", job_id)
            return
        if job.status not in {"pendente", "distribuindo"}:
            logger.info("Job %s está em %s; solicitação ignorada", job_id, job.status)
            return

    locais = json.loads((BASE_DIR / "dados_bahia.json").read_text(encoding="utf-8"))
    update_job(
        job_id, status="distribuindo", total=len(locais), erro=None,
        iniciado_em=datetime.now(timezone.utc),
    )
    delivery_errors: list[str] = []
    for index, lugar in enumerate(locais):
        payload = {"job_id": job_id, "lugar_id": f"{index:05d}", "lugar": lugar}
        while True:
            try:
                producer.produce(
                    PLACE_TOPIC,
                    key=f"{job_id}:{index:05d}",
                    value=json.dumps(payload, ensure_ascii=False),
                    callback=lambda error, _: delivery_errors.append(str(error)) if error else None,
                )
                producer.poll(0)
                break
            except BufferError:
                producer.poll(1)
    pending = producer.flush(60)
    if pending or delivery_errors:
        raise KafkaException(delivery_errors[0] if delivery_errors else f"{pending} mensagens não entregues")
    with Session(engine) as session:
        job = session.get(ImportJob, job_id)
        if job and job.status not in {"concluido", "falhou"}:
            job.status = "processando"
            session.commit()
    logger.info("Job %s distribuído em %s mensagens", job_id, len(locais))


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": os.getenv("KAFKA_COORDINATOR_GROUP", "turismo-import-coordinator"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")})
    consumer.subscribe([REQUEST_TOPIC])
    logger.info("Coordenador aguardando solicitações em %s", REQUEST_TOPIC)
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                logger.error("Erro do Kafka: %s", message.error())
                continue
            payload = None
            try:
                payload = json.loads(message.value().decode("utf-8"))
                distribute(str(payload["job_id"]), producer)
            except Exception as error:
                job_id = payload.get("job_id") if isinstance(payload, dict) else None
                if job_id:
                    update_job(str(job_id), status="falhou", erro=str(error)[:4000])
                logger.exception("Falha ao distribuir importação")
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
