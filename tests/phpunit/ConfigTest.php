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
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'DROP TABLE IF EXISTS "output"',
                                ],
                            ],
                        ],
                    ],
                ],
                'query_timeout' => 7200,
            ],
        ];
        $configDefinition = new ConfigDefinition();

        $config = new Config($configArray, $configDefinition);

        $this->assertEquals($configArray['parameters'], $config->getParameters());
        $this->assertEquals($configArray['authorization'], $config->getAuthorization());
    }

    public function testMissingBlock(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [],
        ];

        $configDefinition = new ConfigDefinition();
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child node "blocks" at path "root.parameters" must be configured.');
        new Config($configArray, $configDefinition);
    }

    public function testMissingCode(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child node "codes" at path "root.parameters.blocks.0" must be configured.');
        new Config($configArray, $configDefinition);
    }

    public function testMissingScript(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $expectedMessage = 'The child node "script" at path "root.parameters.blocks.0.codes.0" must be configured.';
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);
        new Config($configArray, $configDefinition);
    }

    public function testInvalidQueryTimeout(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'query_timeout' => 'asd',
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'test invalid query',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $expectedMessage = 'Invalid type for path "root.parameters.query_timeout". Expected int, but got string.';
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);
        new Config($configArray, $configDefinition);
    }

    private function getDatabaseConfig(): array
    {
        return [
            'workspace' => [
                'host' => 'xxx',
                'warehouse' => 'xxx',
                'database' => 'xxx',
                'schema' => 'xxx',
                'user' => 'xxx',
                'password' => 'xxx',
            ],
        ];
    }
}
