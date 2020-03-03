<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\Config\BaseConfig;
use Keboola\SnowflakeTransformation\Exception\ApplicationException;

class Config extends BaseConfig
{
    public function getQueryTimeout(): ?int
    {
        try {
            return (int) $this->getValue(['queryTimeout']);
        } catch (\InvalidArgumentException $e) {
            return null;
        }
    }

    public function getSteps(): array
    {
        return $this->getValue(['parameters', 'steps']);
    }

    public function getDatabaseConfig(): array
    {
        try {
            return $this->getValue(['authorization', 'workspace']);
        } catch (\InvalidArgumentException $exception) {
            throw new ApplicationException('Missing authorization for workspace');
        }
    }
}
