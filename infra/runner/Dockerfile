FROM python:3.12-alpine
RUN apk add --no-cache docker-cli
COPY runner.py /runner.py
EXPOSE 9000
CMD ["python", "/runner.py"]
