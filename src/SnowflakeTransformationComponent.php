<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\BaseComponent;
use Keboola\Component\Manifest\ManifestManager;

class SnowflakeTransformationComponent extends BaseComponent
{

    protected function run(): void
    {
        /** @var Config $config */
        $config = $this->getConfig();

        $snowflakeTransformation = new SnowflakeTransformation($config, $this->getLogger());

        $snowflakeTransformation->setSession($config);

        $snowflakeTransformation->processBlocks($config->getBlocks());

        $snowflakeTransformation->createManifestMetadata(
            $config->getExpectedOutputTables(),
            new ManifestManager($this->getDataDir())
        );
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
