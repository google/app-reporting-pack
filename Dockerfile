FROM python:3.8
# Installing from PyPi:
# RUN pip install -e git+https://github.com/google/ads-api-report-fetcher.git#egg=google-ads-api-report-fetcher\&subdirectory=py
ADD requirements.txt .
RUN pip install -r requirements.txt
ADD google_ads_queries/ google_ads_queries/
ADD bq_queries/ bq_queries/
ADD scripts/ .
RUN chmod a+x run-docker.sh
ENTRYPOINT ["./run-docker.sh"]
ENV GOOGLE_APPLICATION_CREDENTIALS service_account.json
CMD ["google_ads_queries/*/*.sql", "bq_queries"]
