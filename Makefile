build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

shell:
	docker exec -it fastapi_app /bin/bash

run:
	poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

test:
	poetry run pytest -v tests/

test-cov:
	poetry run pytest --cov=app --cov-report=term-missing tests/

format:
	poetry run black .

lint:
	poetry run flake8 app tests

check:
	poetry check

clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	rm -rf .mypy_cache .pytest_cache .venv .coverage

restart: down build up
reset: clean down build up

.PHONY: up down restart logs ps build test test-cov lint