#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'orders_user') THEN
            CREATE USER orders_user WITH PASSWORD '${ORDERS_DB_PASSWORD}';
        ELSE
            ALTER USER orders_user WITH PASSWORD '${ORDERS_DB_PASSWORD}';
        END IF;

        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'inventory_user') THEN
            CREATE USER inventory_user WITH PASSWORD '${INVENTORY_DB_PASSWORD:-inventory_pass}';
        ELSE
            ALTER USER inventory_user WITH PASSWORD '${INVENTORY_DB_PASSWORD:-inventory_pass}';
        END IF;
    END
    \$\$;

    SELECT 'CREATE DATABASE orders_db OWNER orders_user'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'orders_db')\gexec

    SELECT 'CREATE DATABASE inventory_db OWNER inventory_user'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'inventory_db')\gexec

    SELECT 'CREATE DATABASE inventory_test OWNER inventory_user'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'inventory_test')\gexec
EOSQL
