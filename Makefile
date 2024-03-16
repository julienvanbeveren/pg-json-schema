.PHONY: db
db:
	bash -c "sudo docker compose up -d"

.PHONY: test
test:
	export DATABASE_URL=postgres://postgres:admin@localhost:5432/db
	pytest tests/draft/*.py --junitxml=./report.xml || true
	python3 results.py
