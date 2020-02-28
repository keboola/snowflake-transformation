<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\Tests;

use Keboola\SnowflakeTransformation\Config;
use Keboola\SnowflakeTransformation\ConfigDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigTest extends TestCase
{
    public function testConfig(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'steps' => [
                    [
                        'name' => 'first step',
                        'execution' => 'serial',
                        'blocks' => [
                            [
                                'name' => 'first block',
                                'script' => [
                                    'DROP TABLE IF EXISTS "output"',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $configDefinition = new ConfigDefinition();

        $config = new Config($configArray, $configDefinition);

        $this->assertEquals($configArray['parameters'], $config->getParameters());
        $this->assertEquals($configArray['authorization'], $config->getAuthorization());
    }

    public function testMissingStep(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [],
        ];

        $configDefinition = new ConfigDefinition();
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child node "steps" at path "root.parameters" must be configured.');
        new Config($configArray, $configDefinition);
    }

    public function testMissingBlock(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'steps' => [
                    [
                        'name' => 'first step',
                        'execution' => 'serial',
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child node "blocks" at path "root.parameters.steps.0" must be configured.');
        new Config($configArray, $configDefinition);
    }

    public function testMissingScript(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'steps' => [
                    [
                        'name' => 'first step',
                        'execution' => 'serial',
                        'blocks' => [
                            [
                                'name' => 'first block',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $expectedMessage = 'The child node "script" at path "root.parameters.steps.0.blocks.0" must be configured.';
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);
        new Config($configArray, $configDefinition);
    }

    public function testInvalidExecutionType(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'steps' => [
                    [
                        'name' => 'first step',
                        'execution' => 'invalidType',
                        'blocks' => [
                            [
                                'name' => 'first block',
                                'script' => [
                                    'DROP TABLE IF EXISTS "output"',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $expectedMessage =
            'The value "invalidType" is not allowed for path "root.parameters.steps.0.execution". ' .
            'Permissible values: "parallel", "serial"';
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);
        new Config($configArray, $configDefinition);
    }

    private function getDatabaseConfig(): array
    {
        return [
            'host' => 'xxx',
            'port' => 'xxx',
            'warehouse' => 'xxx',
            'database' => 'xxx',
            'schema' => 'xxx',
            'user' => 'xxx',
            'password' => 'xxx',
        ];
    }
}
