.PHONY: db
db:
	bash -c "sudo docker compose up"

.PHONY: test
test:
	export DATABASE_URL=postgres://postgres:admin@localhost:5432/db
	pytest tests/**/*.py
