<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\BaseComponent;
use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\UserException;

class SnowflakeTransformationComponent extends BaseComponent
{

    /**
     * @throws \Keboola\Component\Manifest\ManifestManager\Options\OptionsValidationException
     * @throws \Keboola\Component\UserException
     * @throws \Keboola\Datatype\Definition\Exception\InvalidOptionException
     * @throws \Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException
     * @throws \Keboola\SnowflakeTransformation\Exception\ApplicationException
     */
    protected function run(): void
    {
        /** @var Config $config */
        $config = $this->getConfig();

        $snowflakeTransformation = new SnowflakeTransformation($config, $this->getLogger());

        $snowflakeTransformation->setSession($config);

        try {
            $snowflakeTransformation->processBlocks($config->getBlocks());
        } catch (UserException $e) {
            $snowflakeTransformation->createManifestMetadata(
                $config->getExpectedOutputTables(),
                new ManifestManager($this->getDataDir()),
                true
            );
            throw $e;
        }

        /** @var array<array{source: string}> $tables */
        $tables = $config->getExpectedOutputTables();
        $snowflakeTransformation->createManifestMetadata(
            $tables,
            new ManifestManager($this->getDataDir()),
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
