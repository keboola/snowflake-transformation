<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeTransformation\Exception\ApplicationException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;
use Symfony\Component\Serializer\Encoder\JsonEncode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;

abstract class AbstractBaseTest extends TestCase
{
    protected string $dataDir = '/data';

    protected Connection $connection;

    public function setUp(): void
    {
        $databaseConfig = $this->getDatabaseConfig();
        $this->connection = $this->getConnection($databaseConfig['workspace']);
    }

    protected function getDatabaseConfig(): array
    {
        return [
            'workspace' => [
                'host' => $this->getEnv('SNOWFLAKE_HOST'),
                'port' => $this->getEnv('SNOWFLAKE_PORT'),
                'warehouse' => $this->getEnv('SNOWFLAKE_WAREHOUSE'),
                'database' => $this->getEnv('SNOWFLAKE_DATABASE'),
                'schema' => $this->getEnv('SNOWFLAKE_SCHEMA'),
                'user' => $this->getEnv('SNOWFLAKE_USER'),
                'password' => $this->getEnv('SNOWFLAKE_PASSWORD'),
            ],
        ];
    }

    protected function putConfig(array $config): void
    {
        $jsonEncode = new JsonEncode();
        $json = $jsonEncode->encode($config, JsonEncoder::FORMAT);
        file_put_contents($this->dataDir . '/config.json', $json);
    }

    protected function runProcess(array $config): Process
    {
        $this->putConfig($config);
        $process = new Process(['php', __DIR__ . '/../../src/run.php']);
        $process->run();
        return $process;
    }

    protected function getEnv(string $envName): string
    {
        $envValue = getenv($envName);
        if (!$envValue) {
            throw new ApplicationException(sprintf('Missing environment "%s".', $envName));
        }
        return $envValue;
    }

    protected function getConnection(array $databaseConfig): Connection
    {
        return new Connection($databaseConfig);
    }
}
