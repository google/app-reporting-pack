# This is Dockerfile for running "one-click-deployment" - runnign ARP app in Cloud Run 
FROM python:3.10
#TODO: WORKDIR /app

ADD requirements.txt .
# common packages for URP
RUN pip install --require-hashes -r requirements.txt
# TODO: packages for Cloud Run application
#RUN pip install flask &&
#    pip install gunicorn

ADD google_ads_queries/ google_ads_queries/
ADD bq_queries/ bq_queries/
ADD scripts/ .
ADD google-ads.yaml .
ADD config.yaml .
ADD main.py .

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
#CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 gcp.one_click_deployment.main:app