FROM python:3.14-slim
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install --no-install-recommends -y \
    gcc \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /synapsePythonClient
COPY . .

# pip is not needed at runtime and its vendored dependencies trigger CVE findings
RUN pip install --no-cache-dir .[pandas,curator] \
    && pip uninstall -y pip


LABEL org.opencontainers.image.source='https://github.com/Sage-Bionetworks/synapsePythonClient'
