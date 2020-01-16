#!/usr/bin/env bash
set -euo pipefail; [[ "${TRACE-}" ]] && set -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

/spark/bin/spark-submit --class de.hpi.spark_tutorial.Sindy \
                        --master local[8] \
                        --deploy-mode client \
                        --total-executor-cores 2 \
                        --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
                        --conf spark.eventLog.enabled=true \
                        "$DIR"/target/SparkTutorial-1.0.jar