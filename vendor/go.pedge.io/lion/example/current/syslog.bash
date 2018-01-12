#!/bin/bash

set -e

DIR="$(cd "$(dirname "${0}")" && pwd)"
cd "${DIR}"

if [ -z "${1}" ]; then
  echo "error: must specify org as first argument"
  exit 1
fi

export CURRENT_TOKEN="$(current syslog -n "${1}" | grep 'Token:' | sed 's/Token: //')"
export CURRENT_SYSLOG_ADDRESS="$(current syslog -n "${1}" | grep 'Plaintext Syslog:' | sed 's/Plaintext Syslog: //')"

echo "CURRENT_TOKEN: ${CURRENT_TOKEN}"
echo "CURRENT_SYSLOG_ADDRESS: ${CURRENT_SYSLOG_ADDRESS}"

go run main.go main.pb.go
