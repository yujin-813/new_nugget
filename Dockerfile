FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Minimal runtime deps (build tools are intentionally omitted)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure writable runtime dirs
RUN mkdir -p /app/uploaded_files /app/flask_session

EXPOSE 5001

CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:5001", "app:app", "--timeout", "120"]
