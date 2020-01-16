#!/usr/bin/env bash
set -euo pipefail; [[ "${TRACE-}" ]] && set -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

/spark/sbin/start-history-server.sh
sleep infinity