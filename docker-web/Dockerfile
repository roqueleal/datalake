FROM php:fpm-alpine

RUN apk update

# Install Postgre PDO
RUN apk update install -y libpq-dev \
    && docker-php-ext-configure pgsql -with-pgsql=/usr/local/pgsql \
    && docker-php-ext-install pdo pdo_pgsql pgsql
