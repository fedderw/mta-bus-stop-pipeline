setup_python:
	python3.11 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt

format:
	. venv/bin/activate && black . -l 79 && isort .
