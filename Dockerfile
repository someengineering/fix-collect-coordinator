FROM python:3.11-slim

ADD . /coordinator
RUN pip install -r /coordinator/requirements.txt && pip install /coordinator && rm -rf /coordinator

ENTRYPOINT ["collect_coordinator"]
