FROM python:3.10-slim-buster
COPY app/requirements.txt requirements.txt
RUN --mount=type=cache,target=/root/.cache pip install --require-hashes -r requirements.txt --no-deps
COPY app app
ENV GOOGLE_APPLICATION_CREDENTIALS app/service_account.json

ENTRYPOINT ["./app/run-local.sh", "--quiet"]
CMD ["--google-ads-config", "/google-ads.yaml", "--config", "/app_reporting_pack.yaml", "--legacy"]
