# Snowflake transformation

[![Build Status](https://travis-ci.com/keboola/snowflake-transformation.svg?branch=master)](https://travis-ci.com/keboola/snowflake-transformation)

Application which runs KBC transformations

## Options

- `authorization` object (required): [workspace credentials](https://developers.keboola.com/extend/common-interface/folders/#exchanging-data-via-workspace)
- `parameters`
    - `blocks` array (required): list of blocks
        - `name` string (optional): name of the block
        - `codes` array (required): list of codes
            - `name` string (optional): name of the code
            - `script` array (required): list of sql queries

## Example configuration

```json
{
  "authorization": {
    "workspace": {
      "host": "snowflake_host",
      "warehouse": "snowflake_warehouse",
      "database": "snowflake_database",
      "schema": "snowflake_schema",
      "user": "snowflake_user",
      "password": "snowflake_password"
    }
  },
  "parameters": {
    "blocks": [
      {
        "name": "first block",
        "codes": [
          {
            "name": "first code",
            "script": [
              "CREATE TABLE IF NOT EXISTS \"example\" (\"name\" VARCHAR(200),\"usercity\" VARCHAR(200));",
              "INSERT INTO \"example\" VALUES ('test example name', 'Prague'), ('test example name 2', 'Brno'), ('test example name 3', 'Ostrava')"
            ]
          }
        ]
      }
    ]
  }
}
```


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
