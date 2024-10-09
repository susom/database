#!/usr/bin/env bash

# Generate a complex password
PASSWORD=$(openssl rand -base64 18 | tr -d +/ | head -c 20)
#export TZ=Asia/Kolkata
#export TZ=America/Los_Angeles
export TZ=UTC # Set timezone to UTC for better compatibility

run_ms_tests() {
  docker pull mcr.microsoft.com/mssql/server:$1
  docker run -d --rm --name dbtest-ms -e ACCEPT_EULA=Y -e TZ=$TZ -e SA_PASSWORD=$PASSWORD -p 1433:1433 \
    --health-cmd='/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P '$PASSWORD' -Q "SELECT 1"' \
    --health-interval=5s --health-timeout=10s --health-retries=10 \
    mcr.microsoft.com/mssql/server:$1

  declare -i count=1
  while [  "$(docker inspect --format='{{json .State.Health.Status}}' dbtest-ms)" != '"healthy"' ]
  do
    echo "Waiting for container to start ($count seconds)"
    sleep 5

    count=$((count + 5))
    if [ $count -gt 180 ] ; then
      echo "Database did not startup correctly ($1)"
      docker rm -f dbtest-ms
      exit 1
    fi
  done

  mvn -e -Dmaven.javadoc.skip=true \
      -Dfailsafe.rerunFailingTestsCount=2 \
      "-Dsqlserver.database.url=jdbc:sqlserver://localhost:1433" \
      -Dsqlserver.database.user=sa \
      -Dsqlserver.database.password=$PASSWORD \
      -P sqlserver.only,coverage test

  if [ $? -ne 0 ] ; then
    echo "mvn command failed"
    docker rm -f dbtest-ms
    exit 1
  fi

  docker rm -f dbtest-ms
}

# The 2017 image seems to have a problem with daylight savings...
#run_ms_tests 2017-latest
run_ms_tests 2019-latest