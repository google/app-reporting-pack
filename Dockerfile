FROM python:3.10-slim-buster

WORKDIR /app
COPY app/ .
RUN pip install --require-hashes -r requirements.txt --no-deps
ENV GOOGLE_APPLICATION_CREDENTIALS service_account.json

ENTRYPOINT ["./app/run-local.sh", "--quiet"]
CMD ["--google-ads-config", "/google-ads.yaml", "--config", "/app_reporting_pack.yaml", "--legacy"]
