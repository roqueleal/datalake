FROM Debian 
FROM php:fpm-alpine

RUN set -ex \
  && apk --no-cache add \
    postgresql-dev

RUN docker-php-ext-install pdo pdo_pgsql
#chown -R 5050:5050 ./docker-web/pgadmin
