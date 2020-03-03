<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\Config\BaseConfig;

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
        return $this->getValue(['authorization', 'workspace']);
    }
}
