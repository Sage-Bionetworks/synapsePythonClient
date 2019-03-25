FROM ubuntu:18.04
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt-get update && apt-get install -y \
	python3 \
	python3-pip \
	python3-pandas

RUN pip3 install --upgrade pip

ADD . /synapsePythonClient
WORKDIR /synapsePythonClient
RUN python3 setup.py install

