
FROM php:7.0

RUN echo 'deb http://ftp.debian.org/debian jessie-backports main' > /etc/apt/sources.list.d/backports.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
    libpq-dev \
    libxml2-dev \
    unzip \
    build-essential gettext libpq-dev\
    wkhtmltopdf\
    gdal-bin\    
 && apt-get -t jessie-backports install -y --no-install-recommends \
    libav-tools \
 && rm -rf /var/lib/apt/lists/* \
 && docker-php-ext-install \
    pcntl \
    pdo \
    pdo_pgsql \
    pgsql \



#chown -R 5050:5050 ./docker-web/pgadmin
