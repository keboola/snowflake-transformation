<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\DatadirTests;

use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecification;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\SnowflakeTransformation\Config;
use Keboola\SnowflakeTransformation\ConfigDefinition;
use Symfony\Component\Process\Process;

class DatadirTest extends AbstractDatadirTestCase
{
    protected Connection $db;

    public function testTransformData(): void
    {
        // phpcs:disable Generic.Files.LineLength
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'steps' => [
                    [
                        'name' => 'first step',
                        'blocks' => [
                            [
                                'name' => 'first block',
                                'script' => [
                                    'DROP TABLE IF EXISTS "output"',
                                    'CREATE TABLE IF NOT EXISTS "output" ("name" VARCHAR(200),"usercity" VARCHAR(200));',
                                    "INSERT INTO \"output\" VALUES ('ondra', 'liberec'), ('odin', 'brno'), ('najlos', 'liberec')",
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $this->runAppWithConfig($configArray);

        $config = $this->getConfigFromUserConfig($configArray);
        $insertedData = $this->getConnection($config)->fetchAll(
            sprintf('SELECT * FROM %s', QueryBuilder::quoteIdentifier('output'))
        );
        $this->assertEquals($insertedData, [
            [
                'name' => 'ondra',
                'usercity' => 'liberec',
            ],
            [
                'name' => 'odin',
                'usercity' => 'brno',
            ],
            [
                'name' => 'najlos',
                'usercity' => 'liberec',
            ],
        ]);
    }

    public function testQueryFailed(): void
    {
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'steps' => [
                    [
                        'name' => 'first step',
                        'blocks' => [
                            [
                                'name' => 'first block',
                                'script' => [
                                    'test invalid query',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        // phpcs:disable Generic.Files.LineLength
        $expectedMessage = "Query \"test invalid query\" in \"first block\" failed with error: \"Error \"odbc_prepare(): SQL error: SQL compilation error: syntax error line 1 at position 0 unexpected 'test'., SQL state 37000 in SQLPrepare\" while executing query \"test invalid query\"\"\n";
        // phpcs:enable

        $this->runAppWithConfig($config, 1, null, $expectedMessage);
    }

    public function testQueryTimeoutSessionOverride(): void
    {
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'query_timeout' => 5,
                'steps' => [
                    [
                        'name' => 'first step',
                        'blocks' => [
                            [
                                'name' => 'first block',
                                'script' => [
                                    'CALL SYSTEM$WAIT(10);',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $expectedMessage = 'Query "CALL SYSTEM$WAIT(10);" in "first block" failed with error: ' .
            "\"Query reached its timeout 5 second(s)\"\n";
        $this->runAppWithConfig(
            $config,
            1,
            null,
            $expectedMessage
        );
    }

    public function testMissingAuthorization(): void
    {
        $config = [
            'parameters' => [
                'steps' => [
                    [
                        'name' => 'first step',
                        'blocks' => [
                            [
                                'name' => 'first block',
                                'script' => [
                                    'select 1',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $process = $this->runAppWithConfig(
            $config,
            2
        );
        $this->assertStringContainsString('Missing authorization for workspace', $process->getErrorOutput());
    }

    public function testQueryTagging(): void
    {
        $createTableQuery = 'create table "query_tag" (
  "QUERY_TEXT" varchar(200), 
  "QUERY_TAG" varchar(200)
);';
        // phpcs:disable Generic.Files.LineLength
        $configArray = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'steps' => [
                    [
                        'name' => 'first step',
                        'blocks' => [
                            [
                                'name' => 'first block',
                                'script' => [
                                    'drop table if exists "query_tag";',
                                    $createTableQuery,
                                    sprintf('insert into "query_tag" SELECT QUERY_TEXT, QUERY_TAG FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION()) WHERE QUERY_TEXT = \'%s\' ORDER BY START_TIME DESC LIMIT 1;', $createTableQuery),
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $this->runAppWithConfig($configArray);

        $config = $this->getConfigFromUserConfig($configArray);

        $connection = $this->getConnection($config);

        $insertedData = $connection->fetchAll(
            sprintf('SELECT * FROM %s', QueryBuilder::quoteIdentifier('query_tag'))
        );

        $this->assertNotEmpty($insertedData);
        $expectedData = sprintf('{"runId":"%s"}', getenv('KBC_RUNID'));
        $this->assertEquals($expectedData, $insertedData[0]['QUERY_TAG']);
    }

    private function runAppWithConfig(
        array $config,
        int $expectedReturnCode = 0,
        ?string $expectedStdout = null,
        ?string $expectedStderr = null
    ): Process {
        $specification = new DatadirTestSpecification(
            null,
            $expectedReturnCode,
            $expectedStdout,
            $expectedStderr,
            null
        );

        $tempDatadir = $this->getTempDatadir($specification);

        file_put_contents($tempDatadir->getTmpFolder() . '/config.json', json_encode($config));

        $process = $this->runScript($tempDatadir->getTmpFolder());

        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());

        return $process;
    }

    private function getConfigFromUserConfig(array $userConfig): Config
    {
        return new Config($userConfig, new ConfigDefinition());
    }

    private function getConnection(Config $config): Connection
    {
        return new Connection($config->getDatabaseConfig());
    }

    private function getDatabaseConfig(): array
    {
        return [
            'workspace' => [
                'host' => getenv('SNOWFLAKE_HOST'),
                'port' => getenv('SNOWFLAKE_PORT'),
                'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => getenv('SNOWFLAKE_DATABASE'),
                'schema' => getenv('SNOWFLAKE_SCHEMA'),
                'user' => getenv('SNOWFLAKE_USER'),
                'password' => getenv('SNOWFLAKE_PASSWORD'),
            ],
        ];
    }
}
