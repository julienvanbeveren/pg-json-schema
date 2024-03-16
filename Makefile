.PHONY: db
db:
	bash -c "sudo docker compose up -d"

.PHONY: test
test:
	export DATABASE_URL=postgres://postgres:admin@localhost:5432/db
	pytest -n 1 --dist loadscope tests/draft/*.py
