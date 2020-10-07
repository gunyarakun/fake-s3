FROM python:3.9-slim-buster

COPY install-packages.sh .
RUN ./install-packages.sh

ENV APP=/app \
    PYTHONUNBUFFERED=1

WORKDIR $APP

EXPOSE 9000

COPY ./fake-s3/requirements.txt $APP

RUN pip install --upgrade pip && \
  pip install -r requirements.txt

COPY ./fake-s3/ $APP/

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]
