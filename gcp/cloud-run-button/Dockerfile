FROM python:alpine
RUN apk add --update --no-cache py3-pip
EXPOSE 80/tcp
WORKDIR /app
CMD ["python3", "-m", "http.server", "80"]
