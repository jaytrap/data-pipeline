FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./

# No CMD baked in with a specific source.
# Each container instance gets its config via environment variables.
CMD ["python", "consumer.py"]
