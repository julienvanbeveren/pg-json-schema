.PHONY: db
db:
	bash -c "sudo docker compose up -d"

.PHONY: test
test:
	export DATABASE_URL=postgres://postgres:admin@localhost:5432/db
	pytest tests/draft/*.py --junitxml=./report.xml || true
	bash -c "head -n 1 report.xml | awk -F\\\" '/<testsuite/{printf \"{\\\"schemaVersion\\\":1,\\\"label\\\":\\\"tests\\\",\\\"message\\\":\\\"%d passed, %d failed\\\",\\\"color\\\":\\\"red\\\"}\\n\", \$$10-\$$6, \$$6}' > report.json"
