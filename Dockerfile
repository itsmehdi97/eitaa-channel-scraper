FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV WORKDIR=/src
WORKDIR $WORKDIR

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY src $WORKDIR

CMD uvicorn api.server:app --reload --host 0.0.0.0 --port 80 --log-level debug