FROM python:3.11-slim

ADD . /coordinator
RUN pip install /coordinator && rm -rf /coordinator

ENTRYPOINT ["collect_coordinator"]
