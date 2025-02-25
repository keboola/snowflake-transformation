# Snowflake Transformation

[![Build Status](https://travis-ci.com/keboola/snowflake-transformation.svg?branch=master)](https://travis-ci.com/keboola/snowflake-transformation)

This application runs with Keboola transformations.

## Options

- `authorization` object (required): [Workspace credentials](https://developers.keboola.com/extend/common-interface/folders/#exchanging-data-via-workspace)
- `parameters`
    - `blocks` array (required): List of blocks
        - `name` string (required): Name of the block
        - `codes` array (required): List of codes
            - `name` string (required): Name of the code
            - `script` array (required): List of sql queries

## Example Configuration

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
 
Clone this repository and initialize the workspace with the following commands:

```
git clone https://github.com/keboola/snowflake-transformation
cd snowflake-transformation
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Create a `.env` file with the following contents:
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

## License

MIT licensed, see the [LICENSE](./LICENSE) file.
