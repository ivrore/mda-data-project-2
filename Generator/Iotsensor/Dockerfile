FROM python:3.8.16-slim

WORKDIR /app

COPY generator.py generator.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

CMD [ "python", "-u", "generator.py","--project_id","<PROJECT_ID>","--topic_name","<TOPIC_NAME>" ]