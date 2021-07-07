FROM python:3.8.11-alpine
WORKDIR /seed
COPY Pipfile Pipfile.lock seed_es_index.py zip_city_county_state.json core-based-statistical-areas-cbsas-and-combined-statistical-areas-csas.json cities_by_state.json /seed/
ENV ENVIRONMENT=local
ENV STAGE=local
ENV PYTHONPATH="$(dirname "$(pwd)")"
RUN pip install pipenv &&\
    pipenv install --system
