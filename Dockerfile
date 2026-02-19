FROM python:3.12.6-slim

WORKDIR /app

COPY app /app

RUN adduser --system --no-create-home nonroot \
    && pip install --no-cache-dir -r requirements.txt \
    && find /usr/local -type f -name '*.pyc' -delete \
    && find /usr/local -type d -name '__pycache__' -delete \
    && chmod +x /app/run.sh

USER nonroot

ENTRYPOINT ["/app/run.sh"]