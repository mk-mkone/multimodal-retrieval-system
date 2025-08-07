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

## Project Structure
..
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

## Features

- [ ] Ingest structured and unstructured material data
- [ ] Generate embeddings for graph/text/image modalities
- [ ] Store vectors in FAISS for similarity search
- [ ] Serve data and search via FastAPI
- [ ] Optionally visualize results with Streamlit

---

## Getting Started

---

## TODO

---

## Contributions

Pull requests are welcome! Ideas and improvements are greatly appreciated.