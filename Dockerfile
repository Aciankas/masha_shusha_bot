# Устанавливаем версию питона и используемы linux дистрибутив
# Alpine легковесный дистрибутив Linux
FROM python:3.11-alpine3.17

# Устанавливаем нужные приложению зависимости
RUN \
 apk add --no-cache gcc && \
 apk add --no-cache python3 postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc python3-dev musl-dev postgresql-dev && \
 pip install psycopg2 && \
 pip install setuptools==58.2.0 && \
 apk --purge del .build-deps

# Cоздаем директорию, где будет размещено приложение
RUN mkdir /app
WORKDIR /app

# Копируем файлы приложения и запускаем установку пакетов
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY . /app

# При запуске контейнера будет выполнен этот скрипт.
CMD python /app/bot_telegram.py