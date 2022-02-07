FROM php:fpm-alpine
RUN apk update && apk add --no-cache \
        postgresql-dev \
    && docker-php-ext-install -j$(nproc) pgsql \
    && docker-php-ext-install -j$(nproc) pdo_pgsql

#chown -R 5050:5050 ./docker-web/pgadmin
