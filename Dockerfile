FROM python:3.11.11-slim-bookworm
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scripts/ scripts/
COPY tests/ tests/
COPY sql/ sql/
COPY data/demo.csv data/demo.csv
CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-v"]
