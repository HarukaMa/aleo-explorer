FROM postgres:13
LABEL authors="snowtigersoft"

COPY ./pg_dump.sql /docker-entrypoint-initdb.d/init.sql
COPY pg_init.sh /docker-entrypoint-initdb.d/init.sh
