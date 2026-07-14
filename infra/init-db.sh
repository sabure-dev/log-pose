#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER orders_user WITH PASSWORD '${ORDERS_DB_PASSWORD}';
    CREATE DATABASE orders_db OWNER orders_user;
EOSQL