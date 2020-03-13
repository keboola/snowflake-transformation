# Snowflake transformation

[![Build Status](https://travis-ci.com/keboola/snowflake-transformation.svg?branch=master)](https://travis-ci.com/keboola/snowflake-transformation)

Application which runs KBC transformations

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/snowflake-transformation
cd snowflake-transformation
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Create `.env` file with following contents
```
SNOWFLAKE_HOST=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
KBC_RUNID=
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
