# Eva — immagine unica per worker + dispatcher.
# Su Railway si usano DUE servizi dalla stessa immagine, con start command diverso:
#   - Worker:     python agent.py start
#   - Dispatcher: uvicorn dispatcher:app --host 0.0.0.0 --port $PORT
#
# Il CMD di default avvia il worker.

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Pre-scarica i model file (turn-detector multilingue, ecc.) in fase di build.
RUN python agent.py download-files

# Porta del dispatcher (Railway inietta $PORT; default 8080).
EXPOSE 8080

# Default: worker. Per il dispatcher, override del comando su Railway (vedi sopra).
CMD ["python", "agent.py", "start"]
