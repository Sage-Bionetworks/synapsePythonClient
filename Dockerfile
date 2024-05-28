FROM ubuntu:20.04
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    gcc \
    python3 \
    python3-setuptools \
    python3-pip \
    python3-pandas \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip

WORKDIR /synapsePythonClient
COPY . .

RUN pip install --no-cache-dir .


LABEL org.opencontainers.image.source='https://github.com/Sage-Bionetworks/synapsePythonClient'
