FROM python:3.9-slim
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    gcc \
    procps \
    python3-dev \
    python3-setuptools \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip

WORKDIR /synapsePythonClient
COPY . .

RUN pip install --no-cache-dir .[pandas]


LABEL org.opencontainers.image.source='https://github.com/Sage-Bionetworks/synapsePythonClient'
