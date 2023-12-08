# Run main application 
.PHONY: run
run:
	python3 data/src/app.py

.PHONY: create_tables
# Create tables to Postgres 
create_tables:
	python3 SQL/create_tables.py

.PHONY: delete_tables
# delete tables to Postgres 
delete_tables:
	python3 SQL/delete_tables.py

.PHONY: reports
# delete tables to Postgres 
reports:
	python3 SQL/aggregate_reports.py

# Set up to install requirements and create virtual enviornment 
# .PHONY: setup

setup:
		python3 -m venv venv
		./venv/bin/pip install --upgrade pip && \
		./venv/bin/pip install -r requirements_v2.txt