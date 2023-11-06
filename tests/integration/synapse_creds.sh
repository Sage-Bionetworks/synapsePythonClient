#!/usr/bin/env bash

# Inputs
SC_ENDPOINT=$1   # i.e. https://sc.sageit.org
SYNAPSE_PAT=$2   # The Synapse Personal Access Token

# Endpoints
STS_TOKEN_ENDPOINT="${SC_ENDPOINT}/ststoken"

# Get Credentials
AWS_STS_CREDS=$(curl --location-trusted --silent -H "Authorization:Bearer ${SYNAPSE_PAT}"  ${STS_TOKEN_ENDPOINT})

echo ${AWS_STS_CREDS}
