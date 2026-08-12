import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, Producer
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app import ImportJob, LugarImportacao, engine, get_embedding_model

TOPIC = os.getenv("KAFKA_PLACE_TOPIC", "turismo.processar-lugar")
DLQ_TOPIC = os.getenv("KAFKA_PLACE_DLQ_TOPIC", "turismo.processar-lugar.dlq")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("turismo-worker")


def finalize_if_complete(job_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(hashtext(:job_id))"), {"job_id": job_id})
        job = connection.execute(text("SELECT status, total FROM import_jobs WHERE id=:id FOR UPDATE"), {"id": job_id}).mappings().first()
        if not job or job["status"] in {"concluido", "falhou"}:
            return
        ready = connection.scalar(text("SELECT count(*) FROM lugares_importacao WHERE job_id=:id"), {"id": job_id})
        if ready != job["total"]:
            return
        connection.execute(text("UPDATE import_jobs SET status='finalizando' WHERE id=:id"), {"id": job_id})
        connection.execute(text("TRUNCATE TABLE lugares RESTART IDENTITY"))
        connection.execute(text("""
            INSERT INTO lugares (nome, cidade, tipo, is_gratis, tem_acessibilidade, descricao, embedding)
            SELECT nome, cidade, tipo, is_gratis, tem_acessibilidade, descricao, embedding
            FROM lugares_importacao WHERE job_id=:id ORDER BY lugar_id
        """), {"id": job_id})
        connection.execute(text("ANALYZE lugares"))
        connection.execute(text("""
            UPDATE import_jobs SET status='concluido', processados=total,
            concluido_em=now() WHERE id=:id
        """), {"id": job_id})
        connection.execute(text("DELETE FROM lugares_importacao WHERE job_id=:id"), {"id": job_id})
    logger.info("Job %s finalizado", job_id)


def process_place(payload: dict) -> None:
    job_id, lugar_id, lugar = str(payload["job_id"]), str(payload["lugar_id"]), payload["lugar"]
    with Session(engine) as session:
        job = session.get(ImportJob, job_id)
        if not job or job.status in {"concluido", "falhou"}:
            return

    texto = f"passage: {lugar['nome']}. Tipo: {lugar['tipo']} em {lugar['cidade']}. {lugar['descricao']}"
    vetor = get_embedding_model().encode(
        texto, normalize_embeddings=True, show_progress_bar=False
    ).tolist()
    statement = insert(LugarImportacao).values(job_id=job_id, lugar_id=lugar_id, **lugar, embedding=vetor)
    statement = statement.on_conflict_do_nothing(index_elements=["job_id", "lugar_id"])
    with Session(engine) as session:
        result = session.execute(statement)
        if result.rowcount:
            session.execute(text("UPDATE import_jobs SET processados=processados+1 WHERE id=:id"), {"id": job_id})
        session.commit()
    finalize_if_complete(job_id)


def send_to_dlq(producer: Producer, message, error: Exception) -> None:
    producer.produce(DLQ_TOPIC, key=message.key(), value=json.dumps({
        "original_value": message.value().decode("utf-8", errors="replace"),
        "error": str(error), "source_topic": message.topic(), "source_offset": message.offset(),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }))
    producer.flush(10)


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": os.getenv("KAFKA_CONSUMER_GROUP", "turismo-place-workers"),
        "client.id": os.getenv("KAFKA_CLIENT_ID", "turismo-place-worker"),
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
        "max.poll.interval.ms": int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "7200000")),
        "session.timeout.ms": int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000")),
    })
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")})
    consumer.subscribe([TOPIC])
    logger.info("Worker aguardando lugares em %s", TOPIC)
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
                process_place(payload)
            except Exception as error:
                logger.exception("Falha no offset %s; enviando para DLQ", message.offset())
                if isinstance(payload, dict) and payload.get("job_id"):
                    with Session(engine) as session:
                        job = session.get(ImportJob, str(payload["job_id"]))
                        if job and job.status != "concluido":
                            job.status = "falhou"
                            job.erro = str(error)[:4000]
                            job.concluido_em = datetime.now(timezone.utc)
                            session.commit()
                send_to_dlq(producer, message, error)
            try:
                consumer.commit(message=message, asynchronous=False)
            except KafkaException:
                logger.exception("Falha ao confirmar offset %s", message.offset())
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
