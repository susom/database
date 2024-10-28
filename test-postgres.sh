#!/bin/bash

PASSWORD=U.$(openssl rand -base64 18 | tr -d +/)
export TZ=America/Los_Angeles

run_pg_tests() {
  local version=$1
  local port=$2
  docker pull postgres:$version
  docker run -d --rm --name dbtest-pg-$version -e TZ=$TZ -e POSTGRES_PASSWORD=$PASSWORD -p $port:5432 postgres:$version


  # Wait until PostgreSQL is fully ready with pg_isready check
  declare -i count=0
  until docker exec dbtest-pg-$version pg_isready -U postgres -h localhost -p 5432 > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL $version to be ready... ($count seconds)"
    sleep 2
    count=$((count + 2))
    if [ $count -gt 60 ]; then
      echo "Database did not start correctly (version $version on port $port)"
      docker rm -f dbtest-pg-$version
      exit 1
    fi
  done

  # Run the Maven tests
  mvn -e -Dmaven.javadoc.skip=true \
      -Dfailsafe.rerunFailingTestsCount=3 \
      -Dpostgres.database.url=jdbc:postgresql://localhost:$port/postgres \
      -Dpostgres.database.user=postgres \
      -Dpostgres.database.password=$PASSWORD \
      -P postgresql.only,coverage test

  # Check the test result and clean up
  if [ $? -ne 0 ]; then
    echo "Maven command failed for PostgreSQL $version"
    docker rm -f dbtest-pg-$version
    exit 1
  fi

  docker rm -f dbtest-pg-$version
}

# Run tests for each PostgreSQL version on a unique port to avoid conflicts
run_pg_tests 9.6 5432
run_pg_tests 10 5433
run_pg_tests 11 5434
run_pg_tests 12-bullseye 5435
run_pg_tests 13-bullseye 5436
run_pg_tests 14-bullseye 5437
run_pg_tests 15-bullseye 5438