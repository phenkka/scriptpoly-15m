FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app

COPY analyzer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY analyzer /app/analyzer

USER app

CMD ["uvicorn", "analyzer.main:app", "--host", "0.0.0.0", "--port", "8000"]
