from datetime import datetime, timezone
from typing import Iterable, Any, Optional
import uuid
import json
from pathlib import Path

from app.core.db import PostgresClient
from app.core.s3 import S3Client
from app.core.logging_factory import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class Registry:
    """High-level persistence orchestrator (DB + S3)."""

    DDL = """
    CREATE TABLE IF NOT EXISTS ingest_runs (
      id UUID PRIMARY KEY,
      source TEXT NOT NULL,
      started_at TIMESTAMPTZ NOT NULL,
      ended_at TIMESTAMPTZ,
      status TEXT NOT NULL,
      extra JSONB DEFAULT '{}'::jsonb
    );
    CREATE TABLE IF NOT EXISTS raw_assets (
      id UUID PRIMARY KEY,
      run_id UUID REFERENCES ingest_runs(id) ON DELETE SET NULL,
      source TEXT NOT NULL,
      source_id TEXT,
      s3_uri TEXT NOT NULL,
      bytes BIGINT,
      checksum TEXT,
      created_at TIMESTAMPTZ NOT NULL
    );
    CREATE TABLE IF NOT EXISTS documents (
      id UUID PRIMARY KEY,
      kind TEXT NOT NULL,
      source TEXT NOT NULL,
      source_id TEXT,
      material_hash TEXT,
      year INT,
      method TEXT,
      s3_uri TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      metadata JSONB DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS documents_kind_material ON documents(kind, material_hash);
    CREATE INDEX IF NOT EXISTS documents_year ON documents(year);
    CREATE INDEX IF NOT EXISTS documents_metadata_gin ON documents USING GIN (metadata);
    """

    def __init__(self, db: PostgresClient, s3: S3Client) -> None:
        self.db = db
        self.s3 = s3

    def bootstrap(self) -> None:
        logger.info("Bootstrapping registry tables")
        self.db.execute(self.DDL)

    def start_run(self, source: str, extra: Optional[dict] = None) -> str:
        run_id = str(uuid.uuid4())
        logger.info("Starting ingestion run for source=%s with id=%s", source, run_id)
        self.db.execute(
            "INSERT INTO ingest_runs (id, source, started_at, status, extra) VALUES (%s,%s,%s,%s,%s)",
            (
                run_id,
                source,
                datetime.now(timezone.utc),
                "running",
                json.dumps(extra or {}),
            ),
        )
        return run_id

    def end_run(self, run_id: str, status: str = "success") -> None:
        logger.info("Ending run %s with status=%s", run_id, status)
        self.db.execute(
            "UPDATE ingest_runs SET ended_at=%s, status=%s WHERE id=%s",
            (datetime.now(timezone.utc), status, run_id),
        )

    def record_raw(
        self, *, run_id: str | None, source: str, source_id: str | None, local_path: str
    ) -> str:
        logger.info(
            "Recording raw asset for source=%s, source_id=%s, path=%s",
            source,
            source_id,
            local_path,
        )
        size = Path(local_path).stat().st_size
        key = self.s3.make_key(f"raw/{source}")
        uri = self.s3.upload_file(local_path, key)
        self.db.execute(
            "INSERT INTO raw_assets (id, run_id, source, source_id, s3_uri, bytes, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                str(uuid.uuid4()),
                run_id,
                source,
                source_id,
                uri,
                size,
                datetime.now(timezone.utc),
            ),
        )
        return uri

    def record_docs(self, docs: Iterable[Any], *, local_jsonl: str) -> str:
        docs = list(docs)
        logger.info(
            "Recording %d standardized documents from %s", len(docs), local_jsonl
        )
        key = self.s3.make_key("standardized")
        uri = self.s3.upload_file(local_jsonl, key)

        def _json_default(o):
            from datetime import datetime as _dt

            return o.isoformat() if isinstance(o, _dt) else str(o)

        rows = []
        for d in docs:
            if hasattr(d, "model_dump_json"):
                metadata_json = d.model_dump_json()
                payload = d.model_dump()
                created_at = getattr(d, "created_at", payload.get("created_at"))
            elif hasattr(d, "model_dump"):
                payload = d.model_dump()
                metadata_json = json.dumps(payload, default=_json_default)
                created_at = payload.get("created_at")
            else:
                payload = dict(d)
                metadata_json = json.dumps(payload, default=_json_default)
                created_at = payload.get("created_at")

            rows.append(
                (
                    payload["uid"],
                    payload["kind"],
                    payload["source"],
                    payload.get("source_id"),
                    (payload.get("material") or {}).get("material_hash"),
                    payload.get("year"),
                    payload.get("method"),
                    uri,
                    created_at,
                    metadata_json,
                )
            )

        if rows:
            self.db.executemany(
                "INSERT INTO documents (id, kind, source, source_id, material_hash, year, method, s3_uri, created_at, metadata) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING",
                rows,
            )
        return uri
