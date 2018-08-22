FROM ubuntu
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt-get update && apt-get install -y \
	python \
	python-pip \
	python-pandas

RUN pip install --upgrade pip
RUN pip install synapseclient