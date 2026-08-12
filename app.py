import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pgvector.sqlalchemy import VECTOR
from sentence_transformers import SentenceTransformer
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, create_engine, func, select, text
from sqlalchemy.orm import DeclarativeBase, Session

from services.query_parser import apply_deterministic_filters

BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:password@localhost:5432/turismo_db")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-large")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_IMPORT_TOPIC = os.getenv("KAFKA_IMPORT_TOPIC", "turismo.importar-locais")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
embedding_model: SentenceTransformer | None = None
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "client.id": "turismo-api"})


class Base(DeclarativeBase):
    pass


class Lugar(Base):
    __tablename__ = "lugares"

    id = Column(Integer, primary_key=True)
    nome = Column(String(255), nullable=False)
    cidade = Column(String(100), nullable=False)
    tipo = Column(String(80), nullable=False)
    is_gratis = Column(Boolean, nullable=False)
    tem_acessibilidade = Column(Boolean, nullable=False)
    descricao = Column(Text, nullable=False)
    embedding = Column(VECTOR(1024), nullable=False)


class ImportJob(Base):
    __tablename__ = "import_jobs"

    id = Column(String(36), primary_key=True)
    status = Column(String(20), nullable=False, index=True)
    total = Column(Integer, nullable=False, default=0)
    processados = Column(Integer, nullable=False, default=0)
    erro = Column(Text)
    criado_em = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    iniciado_em = Column(DateTime(timezone=True))
    concluido_em = Column(DateTime(timezone=True))


class LugarImportacao(Base):
    __tablename__ = "lugares_importacao"

    job_id = Column(String(36), primary_key=True)
    lugar_id = Column(String(64), primary_key=True)
    nome = Column(String(255), nullable=False)
    cidade = Column(String(100), nullable=False)
    tipo = Column(String(80), nullable=False)
    is_gratis = Column(Boolean, nullable=False)
    tem_acessibilidade = Column(Boolean, nullable=False)
    descricao = Column(Text, nullable=False)
    embedding = Column(VECTOR(1024), nullable=False)


def get_embedding_model() -> SentenceTransformer:
    global embedding_model
    if embedding_model is None:
        embedding_model = SentenceTransformer(EMBEDDING_MODEL)
    return embedding_model


@asynccontextmanager
async def lifespan(_: FastAPI):
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lugares_nome_trgm ON lugares USING gin (nome gin_trgm_ops)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lugares_cidade_trgm ON lugares USING gin (cidade gin_trgm_ops)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lugares_tipo_trgm ON lugares USING gin (tipo gin_trgm_ops)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lugares_descricao_trgm ON lugares USING gin (descricao gin_trgm_ops)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lugares_embedding_hnsw ON lugares USING hnsw (embedding vector_cosine_ops)"))
    yield


app = FastAPI(title="Turismo Inteligente Bahia", version="0.1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(BASE_DIR / "index.html")


@app.get("/health")
def health() -> dict[str, str]:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.post("/popular", status_code=202)
def popular_banco() -> dict[str, Any]:
    arquivo = BASE_DIR / "dados_bahia.json"
    if not arquivo.exists():
        raise HTTPException(500, "Arquivo dados_bahia.json não encontrado")
    locais = json.loads(arquivo.read_text(encoding="utf-8"))
    job_id = str(uuid.uuid4())
    with Session(engine) as session:
        ativo = session.scalar(select(ImportJob.id).where(ImportJob.status.in_(["pendente", "distribuindo", "processando", "finalizando"])).limit(1))
        if ativo:
            raise HTTPException(409, f"Já existe uma importação ativa: {ativo}")
        session.add(ImportJob(id=job_id, status="pendente", total=len(locais)))
        session.commit()
    try:
        delivery_error: list[str] = []
        producer.produce(
            KAFKA_IMPORT_TOPIC,
            key=job_id,
            value=json.dumps({"job_id": job_id}),
            callback=lambda error, _: delivery_error.append(str(error)) if error else None,
        )
        pending = producer.flush(10)
        if pending or delivery_error:
            raise KafkaException(delivery_error[0] if delivery_error else "Timeout ao publicar mensagem")
    except (KafkaException, BufferError) as error:
        with Session(engine) as session:
            job = session.get(ImportJob, job_id)
            job.status, job.erro = "falhou", f"Não foi possível publicar no Kafka: {error}"
            session.commit()
        raise HTTPException(503, "Kafka indisponível") from error
    return {"job_id": job_id, "status": "pendente", "status_url": f"/jobs/{job_id}"}


@app.get("/jobs/{job_id}")
def consultar_job(job_id: str) -> dict[str, Any]:
    with Session(engine) as session:
        job = session.get(ImportJob, job_id)
        if not job:
            raise HTTPException(404, "Job não encontrado")
        percentual = round(job.processados * 100 / job.total, 1) if job.total else 0
        return {
            "job_id": job.id, "status": job.status, "total": job.total,
            "processados": job.processados, "percentual": percentual, "erro": job.erro,
            "criado_em": job.criado_em, "iniciado_em": job.iniciado_em,
            "concluido_em": job.concluido_em,
        }


@app.get("/jobs")
def listar_jobs(limite: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    with Session(engine) as session:
        jobs = session.scalars(select(ImportJob).order_by(ImportJob.criado_em.desc()).limit(limite)).all()
        return [{
            "job_id": job.id, "status": job.status, "total": job.total,
            "processados": job.processados,
            "percentual": round(job.processados * 100 / job.total, 1) if job.total else 0,
            "erro": job.erro, "criado_em": job.criado_em,
        } for job in jobs]


@app.get("/autocomplete")
def autocomplete(q: str = Query(min_length=2, max_length=100)) -> list[dict[str, str]]:
    termo = q.strip()
    padrao = f"%{termo}%"
    sim_nome = func.similarity(Lugar.nome, termo)
    sim_tipo = func.similarity(Lugar.tipo, termo)
    sim_cidade = func.similarity(Lugar.cidade, termo)
    sim_descricao = func.similarity(Lugar.descricao, termo)
    # Nome e tipo têm prioridade; cidade e descrição complementam a descoberta.
    relevancia = func.greatest(
        sim_nome,
        sim_tipo * 0.9,
        sim_cidade * 0.75,
        sim_descricao * 0.45,
    )
    corresponde = (
        Lugar.nome.ilike(padrao)
        | Lugar.tipo.ilike(padrao)
        | Lugar.cidade.ilike(padrao)
        | Lugar.descricao.ilike(padrao)
        | (relevancia > 0.15)
    )
    stmt = (
        select(Lugar.nome, Lugar.cidade, Lugar.tipo)
        .where(corresponde)
        .order_by(relevancia.desc(), Lugar.nome)
        .limit(8)
    )
    with Session(engine) as session:
        return [
            {"nome": nome, "cidade": cidade, "tipo": tipo}
            for nome, cidade, tipo in session.execute(stmt)
        ]


def extrair_filtros(q: str) -> dict[str, Any]:
    prompt = f'''Extraia filtros da busca turística abaixo. Responda somente JSON válido com as chaves
    "cidade" (string ou null), "tipo" (string ou null), "is_gratis" (boolean ou null), "tem_acessibilidade" (boolean ou null)
e "termo_semantico" (string). Busca: {json.dumps(q, ensure_ascii=False)}'''
    try:
        response = requests.post(OLLAMA_URL, json={"model": OLLAMA_MODEL, "prompt": prompt, "format": "json", "stream": False}, timeout=60)
        response.raise_for_status()
        filtros = json.loads(response.json()["response"])
        if not isinstance(filtros, dict):
            filtros = {"termo_semantico": q}
    except (requests.RequestException, KeyError, ValueError, TypeError):
        filtros = {"cidade": None, "is_gratis": None, "tem_acessibilidade": None, "termo_semantico": q}

    with Session(engine) as session:
        cidades = session.scalars(select(Lugar.cidade).distinct()).all()
        tipos = session.scalars(select(Lugar.tipo).distinct()).all()
    return apply_deterministic_filters(q, filtros, cidades, tipos)


@app.get("/buscar")
def buscar_hibrida(q: str = Query(min_length=2, max_length=300)) -> dict[str, Any]:
    filtros = extrair_filtros(q)
    termo = str(filtros.get("termo_semantico") or q)
    vetor = get_embedding_model().encode(f"query: {termo}", normalize_embeddings=True).tolist()
    distancia = Lugar.embedding.cosine_distance(vetor)
    stmt = select(Lugar, distancia.label("distancia"))
    if filtros.get("cidade"):
        stmt = stmt.where(Lugar.cidade.ilike(f"%{filtros['cidade']}%"))
    if filtros.get("tipo"):
        stmt = stmt.where(Lugar.tipo == filtros["tipo"])
    for campo in ("is_gratis", "tem_acessibilidade"):
        if filtros.get(campo) is not None:
            stmt = stmt.where(getattr(Lugar, campo) == bool(filtros[campo]))
    stmt = stmt.order_by(distancia).limit(8)

    with Session(engine) as session:
        rows = session.execute(stmt).all()
    return {"filtros": filtros, "resultados": [
        {"nome": lugar.nome, "cidade": lugar.cidade, "tipo": lugar.tipo,
         "descricao": lugar.descricao, "is_gratis": lugar.is_gratis,
         "tem_acessibilidade": lugar.tem_acessibilidade, "relevancia": round(1 - distancia, 4)}
        for lugar, distancia in rows
    ]}
