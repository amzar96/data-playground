.PHONY: install lint lint-fix format format-fix check check-fix generate ingest \
        transform schema-evolve time-travel query run clean

ifneq (,$(wildcard .env))
include .env
export
endif

PYTHON   := uv run python
PIPELINE := -m src.main
GENCSV   := -m src.scripts.generate_csv
CSV      := data/events.csv
RECORDS  := 5000
SQL       ?= SELECT * FROM events_by_country LIMIT 10
NAMESPACE ?= transformed
TABLE     ?= events_enriched

install:
	uv sync --all-groups

lint:
	uv run ruff check src/

lint-fix:
	uv run ruff check --fix src/

format:
	uv run ruff format --check src/

format-fix:
	uv run ruff format src/

check: lint format

check-fix: lint-fix format-fix

generate:
	$(PYTHON) $(GENCSV) \
		id:uuid name:name email:email city:city country:country \
		company:company phone:phone price:price \
		--records $(RECORDS) \
		--output $(CSV)

ingest:
	$(PYTHON) $(PIPELINE) ingest $(CSV)

transform:
	$(PYTHON) $(PIPELINE) transform

schema-evolve:
	$(PYTHON) $(PIPELINE) schema-evolve --namespace $(NAMESPACE) --table $(TABLE)

time-travel:
	$(PYTHON) $(PIPELINE) time-travel --namespace $(NAMESPACE) --table $(TABLE)

query:
	$(PYTHON) $(PIPELINE) query "$(SQL)"

run: generate ingest transform

clean:
	rm -f $(CSV)
	rm -f *.duckdb
