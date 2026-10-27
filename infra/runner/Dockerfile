# ==================================================================================================
# infra/runner/Dockerfile - image of the optional fault runner (compose profile "faults").
#
# WHAT THIS IS
#   A tiny HTTP server (runner.py) that can stop/start/restart allow-listed MiniShop containers, used by
#   the Scenario Lab for S04 "RabbitMQ outage". It needs the docker CLI and the host's Docker socket
#   (mounted in infra/compose.yaml). Nothing else in MiniShop has Docker access.
#
# Read next: infra/runner/runner.py.
# ==================================================================================================

# Small official Python image based on Alpine Linux (a few MB). runner.py uses only the standard library.
FROM python:3.12-alpine
# The docker command-line client only (no daemon): it talks to the HOST's daemon through the mounted socket.
RUN apk add --no-cache docker-cli
# The whole application is this one script.
COPY runner.py /runner.py
# Documentation: the server listens on 9000 (compose does not publish it to the host on purpose).
EXPOSE 9000
# Start the server.
CMD ["python", "/runner.py"]
