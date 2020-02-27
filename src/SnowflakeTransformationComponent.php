<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\BaseComponent;
use Psr\Log\LoggerInterface;

class SnowflakeTransformationComponent extends BaseComponent
{
    public function __construct(LoggerInterface $logger, ?string $dataDir = null)
    {
        if (!is_null($dataDir)) {
            putenv('KBC_DATADIR=' . $dataDir);
        }
        parent::__construct($logger);
    }

    protected function run(): void
    {
        /** @var Config $config */
        $config = $this->getConfig();

        $snowflakeTransformation = new SnowflakeTransformation($config, $this->getLogger());

        $snowflakeTransformation->setSession($config);

        $snowflakeTransformation->processSteps($config->getSteps());
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
