FROM google/cloud-sdk

WORKDIR /app
COPY ./app/requirements.txt .
RUN python3 -m pip install -r requirements.txt --require-hashes --no-deps

COPY gcp/workload-vm/start.sh start.sh
COPY gcp/workload-vm/main.sh main.sh

# Run the app
ENTRYPOINT ["./main.sh"]
