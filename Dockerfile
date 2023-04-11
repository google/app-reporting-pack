FROM google/cloud-sdk

# Install ttyd
RUN apt-get install -y build-essential cmake git libjson-c-dev libwebsockets-dev
RUN git clone https://github.com/tsl0922/ttyd.git
WORKDIR ./ttyd
RUN cmake . && make && make install

# Install Python
RUN apt-get install python3-pip

# Install ARP with dependencies
EXPOSE 7681/tcp
WORKDIR /app
COPY requirements.txt requirements.txt
RUN python3 -m pip install -r requirements.txt --require-hashes
COPY google_ads_queries/ google_ads_queries/
COPY bq_queries/ bq_queries/
COPY scripts/ scripts/
COPY run-local.sh .
COPY gcp/cloud-run-button/main.sh main.sh
COPY gcp/ gcp/

#NOTE: DO NOT remove the following line
#COPY google-ads.yaml .

CMD ["ttyd", "--url-arg", "bash", "-c", "./main.sh"]
