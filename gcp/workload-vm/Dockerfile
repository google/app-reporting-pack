FROM google/cloud-sdk

WORKDIR /app
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt --require-hashes

#COPY google_ads_queries/ google_ads_queries/
#COPY bq_queries/ bq_queries/
#COPY scripts/ scripts/
COPY run-local.sh .
COPY gcp/workload-vm/main.sh main.sh

# Run the app
ENTRYPOINT ["./main.sh"]
