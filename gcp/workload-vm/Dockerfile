FROM google/cloud-sdk

RUN apt-get install -y python3-venv
ENV VENV=/opt/venv
RUN python3 -m venv $VENV
ENV PATH="$VENV/bin:$PATH"

WORKDIR /app
COPY ./app/requirements.txt .
RUN python3 -m pip install -r requirements.txt --require-hashes --no-deps

COPY gcp/workload-vm/start.sh start.sh
COPY gcp/workload-vm/main.sh main.sh
COPY gcp/settings.ini settings.ini

# Run the app
ENTRYPOINT ["./main.sh"]
