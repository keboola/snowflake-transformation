FROM php:8.4-cli-bullseye

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive

ARG SNOWFLAKE_ODBC_VERSION=3.6.0
ARG SNOWFLAKE_GPG_KEY=2A3149C82551A34A

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR /code/

COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        locales \
        unzip \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        gpg \
        debsig-verify \
        dirmngr \
        gpg-agent \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh

# SNOWFLAKE
COPY ./docker/snowflake/generic.pol /etc/debsig/policies/$SNOWFLAKE_GPG_KEY/generic.pol
COPY ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini



# snowflake - charset settings
ENV LANG en_US.UTF-8

RUN mkdir -p ~/.gnupg \
    && chmod 700 ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && mkdir /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY \
    && if ! gpg --keyserver hkp://keys.gnupg.net --recv-keys $SNOWFLAKE_GPG_KEY; then \
        gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_GPG_KEY;  \
    fi \
    && gpg --export $SNOWFLAKE_GPG_KEY > /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY/debsig.gpg \
    && curl https://sfc-repo.snowflakecomputing.com/odbc/linux/$SNOWFLAKE_ODBC_VERSION/snowflake-odbc-$SNOWFLAKE_ODBC_VERSION.x86_64.deb --output /tmp/snowflake-odbc.deb \
    && debsig-verify /tmp/snowflake-odbc.deb \
    && gpg --batch --delete-key --yes $SNOWFLAKE_GPG_KEY \
    && dpkg -i /tmp/snowflake-odbc.deb \
    && rm /tmp/snowflake-odbc.deb

# Snowflake ODBC
# https://github.com/docker-library/php/issues/103#issuecomment-353674490
RUN set -ex; \
    docker-php-source extract; \
    { \
        echo '# https://github.com/docker-library/php/issues/103#issuecomment-353674490'; \
        echo 'AC_DEFUN([PHP_ALWAYS_SHARED],[])dnl'; \
        echo; \
        cat /usr/src/php/ext/odbc/config.m4; \
    } > temp.m4; \
    mv temp.m4 /usr/src/php/ext/odbc/config.m4; \
    docker-php-ext-configure odbc --with-unixODBC=shared,/usr; \
    docker-php-ext-install odbc; \
    docker-php-source delete

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["php", "/code/src/run.php"]
