FROM python:3

RUN apt -y update \
    && apt -y upgrade \
    && python -m pip install --upgrade pip \
    && useradd -ms /bin/sh etluser

WORKDIR /app
COPY ./src /app


RUN pip install -r requirements.txt

RUN chgrp -R etluser /app && chmod -R g+w /app

USER etluser
