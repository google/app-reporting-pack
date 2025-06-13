FROM python:3.11-slim-buster
COPY --from=ghcr.io/astral-sh/uv:0.5.18 /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_SYSTEM_PYTHON=1
COPY app/requirements.txt requirements.txt
RUN --mount=type=cache,target=/root/.cache uv pip install --require-hashes -r requirements.txt --no-deps
COPY app app
ENV GOOGLE_APPLICATION_CREDENTIALS app/service_account.json

ENTRYPOINT ["./app/run-local.sh", "--quiet"]
CMD ["--google-ads-config", "/google-ads.yaml", "--config", "/app_reporting_pack.yaml", "--legacy"]
