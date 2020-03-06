<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\Config\BaseConfig;
use Keboola\SnowflakeTransformation\Exception\ApplicationException;

class Config extends BaseConfig
{
    public function getQueryTimeout(): int
    {
        return (int) $this->getValue(['parameters', 'query_timeout']);
    }

    public function getBlocks(): array
    {
        return $this->getValue(['parameters', 'blocks']);
    }

    /**
     * @return mixed[][]
     */
    public function getExpectedOutputTables(): array
    {
        return $this->getValue(['storage', 'output'], []);
    }

    public function getDatabaseConfig(): array
    {
        try {
            $databaseConfig = $this->getValue(['authorization', 'workspace']);
            $databaseConfig['clientSessionKeepAlive'] = true;
            return $databaseConfig;
        } catch (\InvalidArgumentException $exception) {
            throw new ApplicationException('Missing authorization for workspace');
        }
    }
}
