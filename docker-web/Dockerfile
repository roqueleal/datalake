FROM php:7.0

RUN apt-get -y update \
    && apt-get install -y build-essential gettext libpq-dev\
    && apt-get install -y wkhtmltopdf\
    && apt-get install -y gdal-bin\
    && apt-get install -y libgdal-dev\
    && apt-get install -y --no-install-recommends software-properties-common\
    && apt-add-repository contrib\
    && apt-get update
    && echo 'deb http://ftp.debian.org/debian jessie-backports main' > /etc/apt/sources.list.d/backports.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    libpq-dev \
    libxml2-dev \
    unzip \
    && apt-get -t jessie-backports install -y --no-install-recommends \
    libav-tools \
    && chown -R 5050:5050 ./docker-web/pgadmin
    && rm -rf /var/lib/apt/lists/* \
    && docker-php-ext-install \
    pcntl \
    pdo \
    pdo_pgsql \
    pgsql \
