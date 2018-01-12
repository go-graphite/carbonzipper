#!/bin/bash

set -e

DIR="$(cd "$(dirname "${0}")" && pwd)"
cd "${DIR}"

if [ -z "${1}" ]; then
  echo "error: must specify org as first argument"
  exit 1
fi

export CURRENT_STDOUT=1

go run main.go main.pb.go | current send -n "${1}"
