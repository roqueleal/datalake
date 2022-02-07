FROM postgres:14-alpine

COPY init.sql /docker-entrypoint-initdb.d/
RUN mkdir -p "$PGDATA" && chmod 700 "$PGDATA"