FROM python:3.8.11-alpine
ADD ./ /seed/
WORKDIR /seed
ENV ENVIRONMENT=local
ENV STAGE=local
ENV PYTHONPATH="$(dirname "$(pwd)")"
RUN pip install pipenv &&\
    pipenv install --system
