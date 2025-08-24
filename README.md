# Multimodal Retrieval System for Materials Discovery

## Objective

Build a production-grade multimodal retrieval system to search and explore materials data from diverse modalities:
- Crystalline structure data (e.g. CIF, POSCAR)
- Scientific publications & patents (text)
- Experimental data (e.g. spectra, images, time series)

The system enables researchers to:
- Retrieve similar materials based on structure or properties
- Perform full-text search on scientific corpora
- Filter/query across structured and unstructured datasets

---

## Architecture Overview

![Architecture](./assets/Architecture.png)

---

## Data Flow Diagram – Ingestion & Indexing

```mermaid
flowchart TD
    A[User/API Trigger] --> B[Raw Data Sources]
    B --> C[Ingestion Module : APIs, Files ]
    C --> D[Preprocessing & Cleaning]
    D --> E[Embedding Generator]
    E --> F[Vector Index : FAISS]
    D --> G[Structured Storage : SQL, NoSQL]
    F & G --> H[Unified Search Interface : FastAPI]
```

---

## Sequence Diagram – User Research

```mermaid
sequenceDiagram
    participant U as User
    participant UI as Streamlit App
    participant API as FastAPI Backend
    participant IDX as Vector Index (FAISS)
    participant DB as Metadata DB

    U->>UI: Submit query
    UI->>API: Send request
    API->>IDX: Search vector similarity
    API->>DB: Fetch metadata
    API->>UI: Return results
    UI->>U: Display results
```

---

## Project Structure

```bash
multimodal-retrieval-system/
├── app/                          # Main application source code
│   ├── api/                      # FastAPI route definitions
│   ├── core/                     # App settings, logging, constants
│   ├── ingestion/                # Data ingestion from APIs and files
│   ├── preprocessing/            # Cleaning and transformation of raw data
│   ├── embedding/                # Embedding models (text, structure, graph)
│   ├── indexing/                 # FAISS/Elastic indexing and search logic
│   ├── models/                   # Data models and schemas
│   ├── services/                 # Business logic (ranking, hybrid search)
│   └── main.py                   # FastAPI entry point
├── data/                         # Local storage for data and embeddings
│   ├── raw/                      # Raw input files
│   ├── processed/                # Cleaned and transformed data
│   └── embeddings/               # Saved embedding vectors
├── notebooks/                    # Jupyter notebooks for prototyping and EDA
├── scripts/                      # CLI utilities and one-off scripts
├── tests/                        # Unit and integration tests
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── README.md
├── .env.example
└── .gitignore
```
---

## Tech Stack

| Layer        | Tech                              |
|--------------|-----------------------------------|
| Ingestion    | Python, Pydantic, PyMuPDF, Pandas |
| Storage      | PostgreSQL, MongoDB, MinIO/S3     |
| Indexing     | FAISS, Sentence-Transformers      |
| API Backend  | FastAPI, Uvicorn, Pydantic        |
| Frontend     | Streamlit                         |
| Infra        | Docker, Docker Compose, Terraform |
| Monitoring   | Prometheus, Grafana, Sentry       |

---

## Configuration & Environment

The application is configured via **environment variables** (loaded by `pydantic-settings`).

Create a local `.env` from the template and adjust values to your setup:

```bash
cp .env.example .env
```

### Core app
- `MRS_APP_NAME` — Application display name.  
- `MRS_ENV` — One of `local|dev|staging|prod`. Drives conservative defaults (debug, logging).  
- `MRS_DEBUG` — `true|false`. Enables verbose errors in development.

### API
- `MRS_API_PREFIX` — Base path for the REST API (default `/api`).  
- `MRS_HOST` — Bind host for Uvicorn (default `0.0.0.0`).  
- `MRS_PORT` — Bind port (default `8000`).

### Storage


### Logging & Observability
- `MRS_LOG_FORMAT` — `json|text`.  
- `MRS_LOG_LEVEL` — `DEBUG|INFO|WARNING|ERROR`.  
- `MRS_LOG_FILE_ENABLED` — `true|false`. If `true`, logs are also written to `LOG_FILE`.  
- `MRS_LOG_FILE` — Path to the log file (e.g. `logs/app.log`).

> **Note:** The default Docker compose maps the service name `postgres` to the PostgreSQL container. If you run the API outside of compose, set `POSTGRES_URI=postgresql://appuser:apppassword@localhost:5432/materials_db` or appropriate host.

### Docker Compose environment

`docker-compose.yml` defines three services:
- `postgres` — persistent data volume `postgres_data`  
- `faiss_indexer` — sidecar holding/initializing FAISS files (volume `faiss_data`)  
- `fastapi_app` — application container (depends on both)
- `minio` — container for storing raw and processed files

You can override env values at runtime:

```bash
ENV=local
LOG_LEVEL=DEBUG
```

---

## Features

- [X] Ingest structured and unstructured material data
- [X] Generate embeddings for graph/text/image modalities
- [X] Store vectors in FAISS for similarity search
- [ ] IN PROGRESS - Serve data and search via FastAPI
- [ ] Visualize results with Streamlit

---

## Getting Started

1. **Clone repo**
   ```bash
   git clone https://github.com/mk-mkone/multimodal-retrieval-system.git
   cd multimodal-retrieval-system
   ```
2. **Install dependencies (Poetry)**
   ```bash
   poetry install
   ```
3. **Run with Docker**
   ```bash
   make build
   make up
   ```

---

## Order Summary

The typical workflow to use the system is:

1. **Ingest** raw materials data via API or file upload.  
2. **Embed** the ingested data to generate vector representations.  
3. **Index** the embeddings into the FAISS vector index.  
4. **Search** using the FastAPI backend to retrieve similar materials.

---

## Execution Workflow

This section outlines the typical workflow for running the multimodal retrieval system, including the necessary prerequisites and commands to execute each stage.

### Prerequisites

Before starting, ensure the following services are running and accessible:

- **PostgreSQL**: For structured data storage.
- **MinIO (or S3 compatible storage)**: For storing raw and processed files.
- **FAISS**: For vector similarity indexing.
- **FastAPI backend**: The API service serving search and ingestion endpoints.

You can start these services using Docker Compose or your preferred method.

### 1. Ingestion

Ingest raw materials data via API or file upload. Example using `curl` to send data to the ingestion endpoint:

```bash
curl -X POST "http://localhost:8000/api/ingest" \
     -H "Content-Type: application/json" \
     -d '{"source": "example_dataset", "data": {...}}'
```

This will trigger the ingestion module to process and store raw data.

### 2. Vectorization (Embedding)

Generate embeddings for the ingested data using the embedding module. Run the embedding script with Poetry:

```bash
poetry run python -m app.embedding.build_text_embeddings
```

Or for structure embeddings:

```bash
poetry run python -m app.embedding.build_structure_embeddings
```

This step converts raw data into vector representations.

### 3. FAISS Index Construction

Build or update the FAISS vector index from the generated embeddings:

```bash
poetry run python -m app.indexing.faiss_index
```

This creates or refreshes the similarity search index used by the API.

### 4. Search API

Run the FastAPI backend to serve search queries and metadata retrieval:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

You can now send search requests to the API:

```bash
curl -X POST "http://localhost:8000/api/search" \
     -H "Content-Type: application/json" \
     -d '{"query": "search term"}'
```


---

## Example Procedure

This section illustrates a full end-to-end workflow of the system, from data ingestion to FAISS indexing and querying via the Search API.

### 0) Execution Prerequisites
- API, PostgreSQL and MinIO services are up (e.g. via Docker Compose).
- Ingestors already produce the pivot file: `data/raw/<kind>/standardized.jsonl`.

---

### 1) Ingestion (writes raw + standardized)

```bash
# Text / publications
curl -X POST http://localhost:8000/ingest/text

# Simulation (DFT / Materials Project / NOMAD…)
curl -X POST http://localhost:8000/ingest/simulation

# Time-series / spectroscopy
curl -X POST http://localhost:8000/ingest/timeseries
```

**Verify:**
- **Database:**  
  ```sql
  SELECT COUNT(*) FROM documents;
  ```
- **Files:**  
  ```
  data/raw/<kind>/raw_*.json
  data/raw/<kind>/standardized.jsonl
  ```

---

### 2) Vectorization (embeddings) from the JSONL pivot

```bash
# Text (SBERT)
poetry run python -m app.embedding.build_text data/raw/text/standardized.jsonl

# Simulation (simple element fingerprint)
poetry run python -m app.embedding.build_sim data/raw/simulation/standardized.jsonl

# Time-series (stats + FFT)
poetry run python -m app.embedding.build_ts data/raw/timeseries/standardized.jsonl
```

**Expected output:**
```
data/embeddings/
  text/<model>/part-000.(parquet|npz)   manifest.json
  simulation/element-hist-20/part-000.* manifest.json
  timeseries/stats-fft16/part-000.*     manifest.json
```

---

### 3) FAISS Index Construction (per modality + model)

```bash
# Example: text, model all-MiniLM-L6-v2
poetry run python -m app.indexing.faiss_index --kind text --model all-MiniLM-L6-v2 --emb-root data/embeddings --out-dir data/index/faiss --metric ip

# Simulation
poetry run python -m app.indexing.faiss_index --kind simulation --model element-hist-20 --emb-root data/embeddings --out-dir data/index/faiss --metric l2

# Time-series
poetry run python -m app.indexing.faiss_index --kind timeseries --model stats-fft16 --emb-root data/embeddings --out-dir data/index/faiss --metric l2
```

**Expected output:**
```
data/index/faiss/<kind>/<model>/
  index.faiss
  ids.npy
```

> **Note:**  
> - For cosine similarity, normalize vectors at embedding time and use `IndexFlatIP` (`--metric ip`).  
> - For raw feature vectors (stats/fingerprints), `l2` is sufficient for the MVP.

---

### 4) Search API

```bash
curl -X POST http://localhost:8000/search -H 'content-type: application/json' -d '{
    "kind": "text",
    "query": "graphene superconductivity",
    "top_k": 50,
    "page": 1,
    "size": 10,
    "filters": {"year_from": 2015}
  }'
```

Other useful endpoints:
```bash
curl http://localhost:8000/search/materials/<DOC_ID>
curl http://localhost:8000/search/metadata
```

---

### 5) Order

1. `/ingest/<kind>` → writes `standardized.jsonl`  
2. `build_<kind>` → writes `data/embeddings/<kind>/<model>/part-000.*` + `manifest.json`  
3. `faiss_index --kind --model` → writes `index.faiss` + `ids.npy`  
4. `/search` → encodes query → FAISS top-k → SQL filters → pagination  

---

- **Logs:** API logs should contain `search_done` with latency and number of results.

---

## TODO

- [ ] Write unit and integration tests for feature indexing + search API.
- [ ] Add monitoring stack (Prometheus, Grafana, Sentry) to observe system performance and errors.
- [ ] Set up CI/CD (GitHub Actions) for linting, testing, and deployment.
- [ ] Complete documentation.

---

## Contributions

Pull requests are welcome! Ideas and improvements are greatly appreciated.