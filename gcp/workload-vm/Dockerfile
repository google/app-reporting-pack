FROM google/cloud-sdk

WORKDIR /app
COPY requirements.new.txt requirements.txt
#TODO: RUN pip install --require-hashes -r requirements.txt (not working)
RUN python3 -m pip install -r requirements.txt

COPY google_ads_queries/ google_ads_queries/
COPY bq_queries/ bq_queries/
COPY scripts/ scripts/
COPY run-docker.sh .
COPY google-ads.yaml . # TODO: remove
COPY config.yaml .  # TODO: remove
COPY gcp/workload-vm/main.sh main.sh

# Run the app
ENTRYPOINT ["./main.sh"]
