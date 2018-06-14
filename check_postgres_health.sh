#!/bin/bash
num_retry=0
until [[ "$num_retry" -gt "$DB_HLTH_CHK_MAX_RETRY" ]]
do
  echo "retry-$num_retry to check health of Postgres DB"
  num_retry=$((num_retry+1))
  RESP=$(PGPASSWORD=$POSTGRES_PASSWORD psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -h "$POSTGRES_SERVER" -c '\dt' > /dev/null 2>&1; echo $?)
  if [[ "$RESP" == 0 ]]; then
    echo "Postgres DB is healthy"
    exit 0
  fi
  sleep "$DB_HLTH_CHK_SLEEP"
done

echo "Postgres DB is not healthy"
exit 1
