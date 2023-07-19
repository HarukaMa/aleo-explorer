#!/bin/bash
set -e

export PGPASSWORD=$POSTGRES_PASSWORD

if psql -h localhost -U $POSTGRES_USER -tAc "SELECT 1 FROM pg_namespace WHERE nspname = 'explorer'" | grep -q 1 ; then
    echo "Schema exists"
else
    echo "Schema does not exist. Running init.sql script."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/init.sql
fi
