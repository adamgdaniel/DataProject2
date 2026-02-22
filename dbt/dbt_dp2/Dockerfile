FROM ghcr.io/dbt-labs/dbt-bigquery:1.7.latest

WORKDIR /usr/app
COPY . .

RUN dbt deps

CMD ["run", "--profiles-dir", ".", "--target", "dev"]