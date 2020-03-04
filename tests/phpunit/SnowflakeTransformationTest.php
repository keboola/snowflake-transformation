<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\Tests;

use Keboola\Component\Logger;
use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\SnowflakeTransformation\Exception\ApplicationException;
use Keboola\SnowflakeTransformation\SnowflakeTransformationComponent;

class SnowflakeTransformationTest extends AbstractBaseTest
{

    public function testTransformData(): void
    {
        // phpcs:disable Generic.Files.LineLength
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

        $process = $this->runProcess($config);

        $this->assertEquals(0, $process->getExitCode(), $process->getErrorOutput());
        $this->assertEmpty($process->getErrorOutput(), $process->getErrorOutput());

        $insertedData = $this->connection->fetchAll(
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

        $this->putConfig($config);
        $logger = new Logger();
        $snowflakeTransformation = new SnowflakeTransformationComponent($logger);

        // phpcs:disable Generic.Files.LineLength
        $expectMessage = 'Query "test invalid query" in "first block" failed with error: "Error "odbc_prepare(): SQL error: SQL compilation error:
syntax error line 1 at position 0 unexpected \'test\'., SQL state 37000 in SQLPrepare" while executing query "test invalid query""';
        // phpcs:enable
        $this->expectException(UserException::class);
        $this->expectExceptionMessage($expectMessage);
        $snowflakeTransformation->execute();
    }

    public function testQueryTimeoutSessionOverride()
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
                                    'CALL SYSTEM$WAIT(10);'
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $this->putConfig($config);
        $logger = new Logger();
        $snowflakeTransformation = new SnowflakeTransformationComponent($logger);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Query "CALL SYSTEM$WAIT(10);" in "first block" failed with error: "Query reached its timeout 5 second(s)"');
        $snowflakeTransformation->execute();
    }

    public function testQueryTagging(): void
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
                                    'drop table if exists "query_tag";',
                                    'create table "query_tag" ("QUERY_TEXT" varchar(200), "QUERY_TAG" varchar(200));',
                                    'insert into "query_tag" SELECT QUERY_TEXT, QUERY_TAG FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION()) WHERE QUERY_TEXT = \'create table "query_tag" ("QUERY_TEXT" varchar(200), "QUERY_TAG" varchar(200));\' ORDER BY START_TIME DESC LIMIT 1;'
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $this->putConfig($config);

        $logger = new Logger();
        $snowflakeTransformation = new SnowflakeTransformationComponent($logger);
        $snowflakeTransformation->execute();

        $insertedData = $this->getConnection($this->getDatabaseConfig()['workspace'])->fetchAll(
            sprintf('SELECT * FROM %s', QueryBuilder::quoteIdentifier('query_tag'))
        );

        $this->assertNotEmpty($insertedData);
        $expectedData = sprintf('{"runId":"%s"}', $this->getEnv('KBC_RUNID'));
        $this->assertEquals($expectedData, $insertedData[0]['QUERY_TAG']);
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

        $this->putConfig($config);
        $logger = new Logger();
        $snowflakeTransformation = new SnowflakeTransformationComponent($logger);

        $this->expectException(ApplicationException::class);
        $this->expectExceptionMessage('Missing authorization for workspace');
        $snowflakeTransformation->execute();
    }
}
