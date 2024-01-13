setup_python:
	python3.11 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt

format:
	. venv/bin/activate && black . -l 79 && isort .

run_pipeline:
	. venv/bin/activate && python main.py --s3-bucket "transitscope-baltimore" --s3-key "mta_bus_stops.parquet"
