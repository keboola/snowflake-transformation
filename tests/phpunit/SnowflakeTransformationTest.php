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
                        'execution' => 'serial',
                        'blocks' => [
                            [
                                'name' => 'first block',
                                'script' => [
                                    'DROP TABLE IF EXISTS "output"',
                                    'CREATE TABLE IF NOT EXISTS "output" ("usergender" VARCHAR(200),"usercity" VARCHAR(200),"usersentiment" VARCHAR(200),"zipcode" VARCHAR(200),"sku" VARCHAR(200),"createdat" VARCHAR(200),"category" VARCHAR(200),"price" VARCHAR(200),"county" VARCHAR(200),"countycode" VARCHAR(200),"userstate" VARCHAR(200),"categorygroup" VARCHAR(200));',
                                    'INSERT INTO "output" SELECT "s"."usergender","s"."usercity","s"."usersentiment","s"."zipcode","s"."sku","s"."createdat","s"."category","s"."price","s"."county","s"."countycode","s"."userstate","c"."name" FROM "sales" AS "s" LEFT JOIN "categories" AS "c" ON "s"."categorygroupcode" = "c"."code"',
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
                        'execution' => 'serial',
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
        $this->expectExceptionMessage('[first block] Query failed: "test invalid query"');
        $snowflakeTransformation->execute();
    }
}
