FROM google/cloud-sdk

WORKDIR /app
COPY requirements.new.txt requirements.txt
#TODO: RUN pip install --require-hashes -r requirements.txt (not working)
RUN python3 -m pip install -r requirements.txt

COPY google_ads_queries/ google_ads_queries/
COPY bq_queries/ bq_queries/
COPY scripts/ scripts/
COPY run-docker.sh .
COPY run-local.sh . # TODO: use run-local.sh instead of run-docker.sh when it works
COPY gcp/workload-vm/main.sh main.sh

# Run the app
ENTRYPOINT ["./main.sh"]
