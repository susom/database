#!/bin/bash

PASSWORD=$(openssl rand -base64 18 | tr -d +/)
#export TZ=Asia/Kolkata
export TZ=America/Los_Angeles

ORACLE_NAME=dbtest-ora

stop_oracle() {
  docker rm -f $ORACLE_NAME
}

start_oracle() {
  docker pull us-west1-docker.pkg.dev/som-rit-infrastructure-prod/third-party/oracledb:$1
  # Supposedly we could set -e ORACLE_PWD=$PASSWORD here, but it doesn't seem to work
  docker run -d --rm --name $ORACLE_NAME -p 1521:1521 -p 5500:5500 us-west1-docker.pkg.dev/som-rit-infrastructure-prod/third-party/oracledb:$1

  if [ $? -ne 0 ] ; then
    echo "Unable to start Oracle docker ($1)"
    exit 1
  fi

  declare -i count=1
  while [  "$(docker inspect --format='{{json .State.Health.Status}}' $ORACLE_NAME)" != '"healthy"' ]
  do
    echo "Waiting for container to start ($count seconds)"
    sleep 1

    count=$((count + 1))
    if [ $count -gt 120 ] ; then
      echo "Database did not startup correctly ($1)"
      stop_oracle
      exit 1
    fi
  done

  docker cp oracledb.sql dbtest-ora:/home/oracle/oracledb.sql
  docker exec $ORACLE_NAME sqlplus / AS SYSDBA @/home/oracle/oracledb.sql
}

test_oracle() {
  mvn -e -Dmaven.javadoc.skip=true \
      -Dfailsafe.rerunFailingTestsCount=2 \
      "-Ddatabase.url=jdbc:oracle:thin:@localhost:1521:ORCLCDB" \
      -Ddatabase.user=testuser \
      -Ddatabase.password="TestPassword456" \
      -P oracle12.only,coverage test
}

run_oracle_tests() {
  start_oracle $1

  test_oracle

  if [ $? -ne 0 ] ; then
    echo "Test mvn command failed"
    stop_oracle
    exit 1
  fi

  stop_oracle
}

run_oracle_tests "19.3-quick"
#run_oracle_tests 2019-latest