#!/bin/bash
num_retry=0
until [[ "$num_retry" -gt "$DB_HLTH_CHK_MAX_RETRY" ]]
do
  echo "retry-$num_retry to check health of Oracle DB"
  num_retry=$((num_retry+1))
  DB_HEALTH="$(docker inspect --format='{{json .State.Health.Status}}' "$ORACLEDB_SERVER")"
  echo "Oracle DB is $DB_HEALTH"
  if [[ "${DB_HEALTH}" == "\"healthy\"" ]]; then
    exit 0
  fi
  sleep "$DB_HLTH_CHK_SLEEP"
done

exit 1
