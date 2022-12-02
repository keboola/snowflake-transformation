<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use InvalidArgumentException;
use Keboola\Component\Config\BaseConfig;
use Keboola\SnowflakeTransformation\Exception\ApplicationException;

class Config extends BaseConfig
{
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
            $databaseConfig = array_intersect_key($databaseConfig, array_fill_keys([
                'host',
                'port',
                'user',
                'password',
                'warehouse',
                'database',
                'schema',
            ], true));
            $databaseConfig['clientSessionKeepAlive'] = true;

            return $databaseConfig;
        } catch (InvalidArgumentException $exception) {
            throw new ApplicationException('Missing authorization for workspace');
        }
    }
}
