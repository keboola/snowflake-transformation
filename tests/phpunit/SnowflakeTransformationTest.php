<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\Tests;

use Keboola\Component\Logger;
use Keboola\Component\UserException;
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

        $this->putConfig($config, $this->dataDir);
        $logger = new Logger();
        $snowflakeTransformation = new SnowflakeTransformationComponent($logger, $this->dataDir);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Query "test invalid query" in "first block" failed with error: "Error "odbc_prepare(): SQL error: SQL compilation error:
syntax error line 1 at position 0 unexpected \'test\'., SQL state 37000 in SQLPrepare" while executing query "test invalid query""');
        $snowflakeTransformation->execute();
    }
}
