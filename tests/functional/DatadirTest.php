<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\DatadirTests;

use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecification;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use PHPUnit\Framework\Assert;
use Symfony\Component\Process\Process;

class DatadirTest extends AbstractDatadirTestCase
{
    public function testTransformData(): void
    {
        // phpcs:disable Generic.Files.LineLength
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
        $connection = new Connection($configArray['authorization']['workspace']);
        $insertedData = $connection->fetchAll(
            sprintf('SELECT * FROM %s', QueryBuilder::quoteIdentifier('output')),
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

    public function testTransformDataWithKeyPair(): void
    {
        $config = $this->getDatabaseConfig();
        $config['workspace']['password'] = '';
        $config['workspace']['privateKey'] = getenv('SNOWFLAKE_PRIVATEKEY');

        // phpcs:disable Generic.Files.LineLength
        $configArray = [
            'authorization' => $config,
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
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
        $connection = new Connection($configArray['authorization']['workspace']);
        $insertedData = $connection->fetchAll(
            sprintf('SELECT * FROM %s', QueryBuilder::quoteIdentifier('output')),
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

        // phpcs:disable Generic.Files.LineLength
        $expectedMessage = "Query \"test invalid query\" in \"first code\" failed with error: \"Error \"odbc_prepare(): SQL error: SQL compilation error: syntax error line 1 at position 0 unexpected 'test'., SQL state 37000 in SQLPrepare\" while executing query \"test invalid query\"\"\n";
        // phpcs:enable

        $this->runAppWithConfig($config, 1, null, $expectedMessage);
    }

    public function testQueryTimeoutSessionOverride(): void
    {
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'query_timeout' => 5,
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'CALL SYSTEM$WAIT(10);',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $expectedMessage = 'Query "CALL SYSTEM$WAIT(10);" in "first code" failed with error: ' .
            "\"Query reached its timeout 5 second(s)\"\n";
        $this->runAppWithConfig(
            $config,
            1,
            null,
            $expectedMessage,
        );
    }

    public function testManifestMetadata(): void
    {
        // phpcs:disable Generic.Files.LineLength
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'source' => 'testmetadata',
                            'destination' => 'out.c-my.testmetadata',
                        ],
                    ],
                    'data_type_support' => 'none',
                ],
            ],
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'drop table if exists "testmetadata";',
                                    'create table "testmetadata" (id int AUTOINCREMENT PRIMARY KEY, name varchar(200), notnull VARCHAR(200) NOT NULL, numeric NUMERIC, decimal DECIMAL(10,2));',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $this->runAppWithConfig($config);

        $manifestFilePath = $this->temp->getTmpFolder() . '/out/tables/testmetadata.manifest';
        $manifestData = (array) json_decode((string) file_get_contents($manifestFilePath), true);
        $this->assertArrayHasKey('metadata', $manifestData);
        $this->assertArrayHasKey('column_metadata', $manifestData);

        $expectedTableMetadata = [
            [
                'key' => 'KBC.name',
                'value' => 'testmetadata',
            ],
            [
                'key' => 'KBC.datatype.backend',
                'value' => 'snowflake',
            ],
        ];

        $expectedColumnMetadata = $this->getExpectedColumnMetadata();

        $expectedColumns = [
            'ID',
            'NAME',
            'NOTNULL',
            'NUMERIC',
            'DECIMAL',
        ];

        $this->assertEquals($expectedTableMetadata, $manifestData['metadata']);
        $this->assertEquals($expectedColumnMetadata, $manifestData['column_metadata']);
        $this->assertEquals($expectedColumns, $manifestData['columns']);
    }

    public function testManifestMetadataWithAuthoritativeDataTypes(): void
    {
        // phpcs:disable Generic.Files.LineLength
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'source' => 'testmetadata',
                            'destination' => 'out.c-my.testmetadata',
                        ],
                    ],
                    'data_type_support' => 'authoritative',
                ],
            ],
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'drop table if exists "testmetadata";',
                                    'create table "testmetadata" (id int AUTOINCREMENT PRIMARY KEY, name varchar(200), notnull VARCHAR(200) NOT NULL, numeric NUMERIC, decimal DECIMAL(10,2));',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $this->runAppWithConfig($config);

        $manifestFilePath = $this->temp->getTmpFolder() . '/out/tables/testmetadata.manifest';
        $manifestData = (array) json_decode((string) file_get_contents($manifestFilePath), true);
        $this->assertArrayHasKey('manifest_type', $manifestData);
        $this->assertArrayHasKey('table_metadata', $manifestData);
        $this->assertArrayHasKey('schema', $manifestData);

        $this->assertSame('output', $manifestData['manifest_type']);

        $expectedTableMetadata = [
            'KBC.name' => 'testmetadata',
            'KBC.datatype.backend' => 'snowflake',
        ];

        $this->assertSame($expectedTableMetadata, $manifestData['table_metadata']);

        $expectedSchema = [
            [
                'nullable' => false,
                'primary_key' => false,
                'metadata' => [],
                'name' => 'ID',
                'data_type' => [
                    'base' => [
                        'default' => 'IDENTITY START 1 INCREMENT 1 NOORDER',
                        'type' => 'NUMERIC',
                    ],
                    'snowflake' => [
                        'default' => 'IDENTITY START 1 INCREMENT 1 NOORDER',
                        'length' => '38,0',
                        'type' => 'NUMBER',
                    ],
                ],
            ],
            [
                'nullable' => true,
                'primary_key' => false,
                'metadata' => [],
                'name' => 'NAME',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                    'snowflake' => [
                        'length' => '200',
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
            [
                'nullable' => false,
                'primary_key' => false,
                'metadata' => [],
                'name' => 'NOTNULL',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                    'snowflake' => [
                        'length' => '200',
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
            [
                'nullable' => true,
                'primary_key' => false,
                'metadata' => [],
                'name' => 'NUMERIC',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                    'snowflake' => [
                        'length' => '38,0',
                        'type' => 'NUMBER',
                    ],
                ],
            ],
            [
                'nullable' => true,
                'primary_key' => false,
                'metadata' => [],
                'name' => 'DECIMAL',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                    'snowflake' => [
                        'length' => '10,2',
                        'type' => 'NUMBER',
                    ],
                ],
            ],
        ];

        $this->assertSame($expectedSchema, $manifestData['schema']);
    }

    public function testManifestMetadataWithTableNameExistingInAnotherSchema(): void
    {
        // phpcs:disable Generic.Files.LineLength
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'source' => 'TABLES',
                            'destination' => 'out.c-my.TABLES',
                        ],
                    ],
                    'data_type_support' => 'none',
                ],
            ],
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'drop table if exists "TABLES";',
                                    'create table "TABLES" (id int AUTOINCREMENT PRIMARY KEY, name varchar(200), notnull VARCHAR(200) NOT NULL, numeric NUMERIC, decimal DECIMAL(10,2));',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $this->runAppWithConfig($config);

        $manifestFilePath = $this->temp->getTmpFolder() . '/out/tables/TABLES.manifest';
        $manifestData = (array) json_decode((string) file_get_contents($manifestFilePath), true);
        $this->assertArrayHasKey('metadata', $manifestData);
        $this->assertArrayHasKey('column_metadata', $manifestData);

        $expectedTableMetadata = [
            [
                'key' => 'KBC.name',
                'value' => 'TABLES',
            ],
            [
                'key' => 'KBC.datatype.backend',
                'value' => 'snowflake',
            ],
        ];

        $expectedColumnMetadata = $this->getExpectedColumnMetadata();

        $expectedColumns = [
            'ID',
            'NAME',
            'NOTNULL',
            'NUMERIC',
            'DECIMAL',
        ];

        $this->assertEquals($expectedTableMetadata, $manifestData['metadata']);
        $this->assertEquals($expectedColumnMetadata, $manifestData['column_metadata']);
        $this->assertEquals($expectedColumns, $manifestData['columns']);
    }

    public function testAbortTransformationWithoutVariable(): void
    {
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'create table if not exists "ABORT_TRANSFORMATION" (id int);',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $process = $this->runAppWithConfig(
            $config,
            0,
        );

        $this->assertStringContainsString('Checking user termination', $process->getOutput());
    }

    public function testInvalidManifestMetadata(): void
    {
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'source' => 'invalid_testmetadata',
                            'destination' => 'out.c-my.invalid_testmetadata',
                        ],
                    ],
                ],
            ],
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $this->runAppWithConfig(
            $config,
            1,
            null,
            "Tables \"invalid_testmetadata\" specified in output were not created by the transformation.\n",
        );
    }

    public function testMissingAuthorization(): void
    {
        $config = [
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
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
            2,
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
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
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

        $connection = new Connection($configArray['authorization']['workspace']);

        $insertedData = $connection->fetchAll(
            sprintf('SELECT * FROM %s', QueryBuilder::quoteIdentifier('query_tag')),
        );

        $this->assertNotEmpty($insertedData);
        $expectedData = sprintf('{"runId":"%s"}', getenv('KBC_RUNID'));
        $this->assertEquals($expectedData, $insertedData[0]['QUERY_TAG']);
    }

    public function testAbortTransformation(): void
    {
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'SET ABORT_TRANSFORMATION=\'Abort Me Please\'',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $this->runAppWithConfig(
            $config,
            1,
            null,
            "Transformation aborted with message \"Abort Me Please\"\n",
        );
    }

    public function testSkipQuery(): void
    {
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    '',
                                    'SELECT 1',
                                    'select 1',
                                    '# create table if not exists "test" (id int)',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $process = $this->runAppWithConfig($config);

        $this->assertEquals(2, substr_count($process->getOutput(), 'Running query'));
    }

    public function testCastOperatorQuery(): void
    {
        // phpcs:disable Generic.Files.LineLength
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'DROP TABLE IF EXISTS accounts;',
                                    'CREATE TABLE accounts ("account_id" varchar);',
                                    'INSERT INTO accounts VALUES (\'123\');',
                                    'CREATE OR REPLACE TABLE "CxAlloy_Projects" AS SELECT "account_id"::INT as account_id from accounts',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $this->runAppWithConfig($config);
    }

    public function testCollateVarchar(): void
    {
        // phpcs:disable Generic.Files.LineLength
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'DROP TABLE IF EXISTS "accounts";',
                                    'CREATE TABLE "accounts" ("account_id" varchar(255) COLLATE \'cz\');',
                                    'INSERT INTO "accounts" VALUES (\'123\');',
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
                            'source' => 'accounts',
                            'destination' => 'out.c-my.accounts',
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $process = $this->runAppWithConfig($config);

        Assert::assertEquals(0, $process->getExitCode());
    }

    public function testIntColumn(): void
    {
        // phpcs:disable Generic.Files.LineLength
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'DROP TABLE IF EXISTS "accounts";',
                                    'CREATE TABLE "accounts" ("1234" int);',
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
                            'source' => 'accounts',
                            'destination' => 'out.c-my.accounts',
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $this->runAppWithConfig($config);

        $manifestFilePath = $this->temp->getTmpFolder() . '/out/tables/accounts.manifest';
        $manifestData = (array) json_decode((string) file_get_contents($manifestFilePath), true);

        $this->assertEquals(['1234'], $manifestData['columns']);
    }

    private function runAppWithConfig(
        array $config,
        int $expectedReturnCode = 0,
        ?string $expectedStdout = null,
        ?string $expectedStderr = null,
    ): Process {
        $specification = new DatadirTestSpecification(
            null,
            $expectedReturnCode,
            $expectedStdout,
            $expectedStderr,
            null,
        );

        $tempDatadir = $this->getTempDatadir($specification);

        file_put_contents($tempDatadir->getTmpFolder() . '/config.json', json_encode($config));

        $process = $this->runScript($tempDatadir->getTmpFolder(), (string) getenv('KBC_RUNID'));

        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());

        return $process;
    }

    private function getDatabaseConfig(): array
    {
        return [
            'workspace' => [
                'host' => getenv('SNOWFLAKE_HOST'),
                'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => getenv('SNOWFLAKE_DATABASE'),
                'schema' => getenv('SNOWFLAKE_SCHEMA'),
                'user' => getenv('SNOWFLAKE_USER'),
                'password' => getenv('SNOWFLAKE_PASSWORD'),
            ],
        ];
    }

    /**
     * @return array[]
     */
    protected function getExpectedColumnMetadata(): array
    {
        return [
            'ID' => [
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'NUMERIC',
                ],
                [
                    'key' => 'KBC.datatype.default',
                    'value' => 'IDENTITY START 1 INCREMENT 1 NOORDER',
                ],
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'NUMBER',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '38,0',
                ],
            ],
            'NAME' => [
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'VARCHAR',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '200',
                ],
            ],
            'NOTNULL' => [
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'VARCHAR',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '200',
                ],
            ],
            'DECIMAL' => [
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'NUMERIC',
                ],
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'NUMBER',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '10,2',
                ],
            ],
            'NUMERIC' => [
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'NUMERIC',
                ],
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'NUMBER',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '38,0',
                ],
            ],
        ];
    }

    public function testAbortTransformationWithWriteAlways(): void
    {
        $config = [
            'authorization' => $this->getDatabaseConfig(),
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'source' => 'accounts',
                            'destination' => 'out.c-my.accounts',
                            'write_always' => true,
                        ],
                        [
                            'source' => 'accounts2',
                            'destination' => 'out.c-my.accounts2',
                            'write_always' => false,
                        ],
                        [
                            'source' => 'accounts3',
                            'destination' => 'out.c-my.accounts3',
                        ],
                    ],
                ],
            ],
            'parameters' => [
                'blocks' => [
                    [
                        'name' => 'first block',
                        'codes' => [
                            [
                                'name' => 'first code',
                                'script' => [
                                    'DROP TABLE IF EXISTS accounts;',
                                    'CREATE TABLE accounts ("account_id" varchar);',
                                    'INSERT INTO accounts VALUES (\'123\');',
                                ],
                            ],
                            [
                                'name' => 'second code',
                                'script' => [
                                    'DROP TABLE IF EXISTS accounts2;',
                                    'CREATE TABLE accounts2 ("account_id" varchar);',
                                    'INSERT INTO accounts2 VALUES (\'123\');',
                                ],
                            ],
                            [
                                'name' => 'third code',
                                'script' => [
                                    'DROP TABLE IF EXISTS accounts3;',
                                    'CREATE TABLE accounts3 ("account_id" varchar);',
                                    'INSERT INTO accounts3 VALUES (\'123\');',
                                ],
                            ],
                            [
                                'name' => 'abort code',
                                'script' => [
                                    'SET ABORT_TRANSFORMATION=\'Abort Me Please\'',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $process = $this->runAppWithConfig(
            $config,
            1,
            null,
            "Transformation aborted with message \"Abort Me Please\"\n",
        );

        $this->assertStringContainsString('Checking user termination', $process->getOutput());

        $manifestFilePath = sprintf('%s/out/tables/accounts.manifest', $this->temp->getTmpFolder());
        $manifestData = (array) json_decode((string) file_get_contents($manifestFilePath), true);
        $this->assertArrayHasKey('metadata', $manifestData);
        $this->assertArrayHasKey('column_metadata', $manifestData);

        $this->assertFileDoesNotExist(sprintf('%s/out/tables/accounts2.manifest', $this->temp->getTmpFolder()));
        $this->assertFileDoesNotExist(sprintf('%s/out/tables/accounts3.manifest', $this->temp->getTmpFolder()));
    }

    public function testEnvVars(): void
    {
        // phpcs:disable Generic.Files.LineLength
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
                                    'DROP TABLE IF EXISTS "envvars"',
                                    'CREATE TABLE IF NOT EXISTS "envvars" ("name" VARCHAR(200), "value" VARCHAR(200));',
                                    'INSERT INTO "envvars" VALUES (\'KBC_RUNID\', (SELECT $KBC_RUNID))',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        // phpcs:enable

        $this->runAppWithConfig($configArray);
        $connection = new Connection($configArray['authorization']['workspace']);
        $insertedData = $connection->fetchAll(
            sprintf('SELECT * FROM %s', QueryBuilder::quoteIdentifier('envvars')),
        );
        $this->assertEquals($insertedData, [
            [
                'name' => 'KBC_RUNID',
                'value' => getenv('KBC_RUNID'),
            ],
        ]);
    }
}
