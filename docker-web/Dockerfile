FROM php:fpm-alpine

RUN apk update

# Install Postgre PDO
RUN apk update install -y libpq-dev \
    && docker-php-ext-configure pgsql -with-pgsql=/usr/local/pgsql \
    && docker-php-ext-install pdo pdo_pgsql pgsql


FROM php:fpm-alpine
RUN apk --update --no-cache upgrade
RUN apk --no-cache add $PHPIZE_DEPS oniguruma-dev libzip-dev curl-dev php8-pecl-mcrypt postgresql-dev

RUN docker-php-ext-install mbstring zip curl iconv

#RUN docker-php-ext-install mysqli pdo pdo_mysql mbstring zip curl iconv
#RUN docker-php-ext-enable pdo_mysql

RUN docker-php-ext-install pdo pdo_pgsql
#RUN docker-php-ext-configure pgsql -with-pgsql=/usr/local/pgsql
RUN docker-php-ext-enable pdo_pgsql
