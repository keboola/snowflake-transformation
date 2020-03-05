<?php

declare(strict_types=1);

use Keboola\SnowflakeTransformation\Exception\ApplicationException;

require __DIR__ . '/../../vendor/autoload.php';

$environments = [
    'SNOWFLAKE_HOST',
    'SNOWFLAKE_PORT',
    'SNOWFLAKE_WAREHOUSE',
    'SNOWFLAKE_DATABASE',
    'SNOWFLAKE_SCHEMA',
    'SNOWFLAKE_USER',
    'SNOWFLAKE_PASSWORD',
    'KBC_RUNID',
];

foreach ($environments as $environment) {
    if (empty(getenv($environment))) {
        throw new ApplicationException(sprintf('Missing environment "%s".', $environment));
    }
}
