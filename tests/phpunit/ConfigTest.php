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
        $this->expectExceptionMessage('The child config "blocks" under "root.parameters" must be configured.');
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
        $this->expectExceptionMessage('The child config "codes" under "root.parameters.blocks.0" must be configured.');
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
        $expectedMessage = 'The child config "script" under "root.parameters.blocks.0.codes.0" must be configured.';
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
        $expectedMessage = 'Invalid type for path "root.parameters.query_timeout". Expected "int", but got "string".';
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);
        new Config($configArray, $configDefinition);
    }

    public function testDatabaseConfigDoesNotContainUnknownKeys(): void
    {
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
        ];
        $configDefinition = new ConfigDefinition();

        $config = new Config($configArray, $configDefinition);

        self::assertArrayNotHasKey('unknownKey', $config->getDatabaseConfig());
    }

    public function testGetExpectedOutputTablesFiltersDirectGrant(): void
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
                                    'SELECT 1',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'source' => 'regular_table',
                            'destination' => 'out.c-my.regular_table',
                        ],
                        [
                            'destination' => 'out.c-my.direct_grant_table',
                            'unload_strategy' => 'direct-grant',
                        ],
                        [
                            'source' => 'table_without_strategy',
                            'destination' => 'out.c-my.table_without_strategy',
                        ],
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $config = new Config($configArray, $configDefinition);

        $expectedTables = $config->getExpectedOutputTables();

        $this->assertCount(2, $expectedTables);

        $sources = array_column($expectedTables, 'source');
        $this->assertContains('regular_table', $sources);
        $this->assertContains('table_without_strategy', $sources);
    }

    public function testGetExpectedOutputTablesWithNoDirectGrant(): void
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
                                    'SELECT 1',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'source' => 'regular_table',
                            'destination' => 'out.c-my.regular_table',
                        ],
                        [
                            'source' => 'another_table',
                            'destination' => 'out.c-my.another_table',
                        ],
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $config = new Config($configArray, $configDefinition);

        $expectedTables = $config->getExpectedOutputTables();

        $this->assertCount(2, $expectedTables);
    }

    public function testGetExpectedOutputTablesWithOnlyDirectGrant(): void
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
                                    'SELECT 1',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'destination' => 'out.c-my.direct_grant_table',
                            'unload_strategy' => 'direct-grant',
                        ],
                        [
                            'destination' => 'out.c-my.another_direct_grant_table',
                            'unload_strategy' => 'direct-grant',
                        ],
                    ],
                ],
            ],
        ];

        $configDefinition = new ConfigDefinition();
        $config = new Config($configArray, $configDefinition);

        $expectedTables = $config->getExpectedOutputTables();

        $this->assertCount(0, $expectedTables);
    }

    /**
     * @return array{
     *     workspace: array{
     *        host: string,
     *        warehouse: string,
     *        database: string,
     *        schema: string,
     *        user: string,
     *        password: string,
     *        unknownKey: string
     *    }
     * }
     */
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
                'unknownKey' => 'xxx',
            ],
        ];
    }
}
