<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use InvalidArgumentException;
use Keboola\Component\Config\BaseConfig;
use Keboola\SnowflakeTransformation\Exception\ApplicationException;

class Config extends BaseConfig
{
    private const SNOWFLAKE_APPLICATION = 'Keboola_Connection';

    public function getQueryTimeout(): int
    {
        return $this->getIntValue(['parameters', 'query_timeout']);
    }

    public function getBlocks(): array
    {
        return $this->getArrayValue(['parameters', 'blocks']);
    }

    public function getDatabaseConfig(): array
    {
        try {
            $databaseConfig = $this->getArrayValue(['authorization', 'workspace']);
            $databaseConfig['password'] ??= '';

            $keysToFill = [
                'host',
                'port',
                'user',
                'password',
                'privateKey',
                'warehouse',
                'database',
                'schema',
            ];

            if (isset($databaseConfig['privateKey'])) {
                $keysToFill[] = 'privateKey';
            }

            $databaseConfig = array_intersect_key($databaseConfig, array_fill_keys($keysToFill, true));
            $databaseConfig['clientSessionKeepAlive'] = true;
            $databaseConfig['application'] = self::SNOWFLAKE_APPLICATION;

            return $databaseConfig;
        } catch (InvalidArgumentException $exception) {
            throw new ApplicationException('Missing authorization for workspace');
        }
    }
}
