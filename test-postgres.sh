#!/bin/bash

PASSWORD=$(openssl rand -base64 18 | tr -d +/)
#export TZ=Asia/Kolkata
export TZ=America/Los_Angeles

run_pg_tests() {
  docker pull postgres:$1
  docker run -d --rm --name dbtest-pg -e TZ=$TZ -e POSTGRES_PASSWORD=$PASSWORD -p 5432:5432/tcp postgres:$1

  declare -i count=1
  while [  "$(docker inspect --format='{{json .State.Status}}' dbtest-pg)" != '"running"' -a $count -lt 10 ]
  do
    echo "Waiting for container to start ($count seconds)"
    sleep 1

    count=$((count + 1))
    if [ $count -gt 120 ] ; then
      echo "Database did not startup correctly ($1)"
      docker rm -f dbtest-pg
      exit 1
    fi
  done

  mvn -e -Dmaven.javadoc.skip=true \
      -Dfailsafe.rerunFailingTestsCount=2 \
      -Dpostgres.database.url=jdbc:postgresql://localhost/postgres \
      -Dpostgres.database.user=postgres \
      -Dpostgres.database.password=$PASSWORD \
      -P postgresql.only,coverage test

  if [ $? -ne 0 ] ; then
    echo "mvn command failed"
    docker rm -f dbtest-pg
    exit 1
  fi

  docker rm -f dbtest-pg
}

run_pg_tests 9.6
run_pg_tests 10
run_pg_tests 11
run_pg_tests 12
run_pg_tests 13
run_pg_tests 14