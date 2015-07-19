#!/bin/bash

# Configure the package manager to find PostgreSQL
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
sudo echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" > /etc/apt/sources

# Install PostgreSQL, configure network access, and restart it
sudo apt-get update && apt-get install -y postgresql-9.3 postgresql-contrib-9.3
sudo -u postgres echo "listen_addresses='*'" >> /etc/postgresql/9.3/main/postgresql.conf
sudo -u postgres echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/9.3/main/pg_hba.conf
sudo service postgresql restart

# Create a database user and database
sudo -u postgres psql --command "CREATE USER vagrant WITH SUPERUSER PASSWORD 'vagrant';"
sudo -u postgres createdb -O vagrant vagrant
