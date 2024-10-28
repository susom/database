#!/bin/bash

PASSWORD=$(openssl rand -base64 18 | tr -d +/)
export TZ=America/Los_Angeles

run_pg_tests() {
  local version=$1
  local container_name="dbtest-pg-$version"

  docker pull postgres:$version
  docker run -d --rm --name $container_name -e TZ=$TZ -e POSTGRES_PASSWORD=$PASSWORD -p 5432:5432 postgres:$version

  # Wait until PostgreSQL is fully ready with pg_isready check
  declare -i count=0
  until docker exec $container_name pg_isready -U postgres -h localhost -p 5432 > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL $version to be ready... ($count seconds)"
    sleep 2
    count=$((count + 2))
    if [ $count -gt 60 ]; then
      echo "Database did not start correctly (version $version)"
      docker rm -f $container_name
      exit 1
    fi
  done

  mvn -e -Dmaven.javadoc.skip=true \
      -Dfailsafe.rerunFailingTestsCount=2 \
      -Dpostgres.database.url=jdbc:postgresql://localhost/postgres \
      -Dpostgres.database.user=postgres \
      -Dpostgres.database.password=$PASSWORD \
      -P postgresql.only,coverage test

  if [ $? -ne 0 ]; then
    echo "Maven command failed for PostgreSQL $version"
    docker rm -f $container_name
    exit 1
  fi

  docker rm -f $container_name
}

run_pg_tests 9.6
run_pg_tests 10
run_pg_tests 11
run_pg_tests 12-bullseye
run_pg_tests 13-bullseye
run_pg_tests 14-bullseye
run_pg_tests 15-bullseye