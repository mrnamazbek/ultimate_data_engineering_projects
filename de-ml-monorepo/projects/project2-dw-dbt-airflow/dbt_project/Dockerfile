FROM python:3.10-slim

RUN pip install --no-cache-dir dbt-postgres==1.8.2

WORKDIR /dbt
COPY . .

CMD ["dbt", "run", "--project-dir", "/dbt", "--profiles-dir", "/dbt"]
