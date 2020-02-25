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

    public function getRunId(): string
    {
        return $this->getValue(['runId']);
    }

    public function getSteps(): array
    {
        return $this->getValue(['parameters', 'steps']);
    }

    public function getDatabaseConfig(): array
    {
        return $this->getValue(['parameters', 'db']);
    }

    public function getDatabaseSchema(): string
    {
        return $this->getValue(['parameters', 'db', 'schema']);
    }
}
