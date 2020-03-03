<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\Tests;

use Keboola\Csv\CsvFile;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;
use Symfony\Component\Serializer\Encoder\JsonEncode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;

abstract class AbstractBaseTest extends TestCase
{
    /** @var string $dataDir */
    protected $dataDir = __DIR__ . '/../data';

    /** @var Connection $connection */
    private $connection;

    public function setUp(): void
    {
        $databaseConfig = $this->getDatabaseConfig();
        $this->connection = $this->getConnection($databaseConfig);

        $this->connection->query(
            sprintf('USE SCHEMA %s', QueryBuilder::quoteIdentifier($databaseConfig['schema']))
        );
    }

    protected function getDatabaseConfig(): array
    {
        return [
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'schema' => getenv('SNOWFLAKE_SCHEMA'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ];
    }

    protected function putConfig(array $config, string $dataDir): void
    {
        $jsonEncode = new JsonEncode();
        $json = $jsonEncode->encode($config, JsonEncoder::FORMAT);
        file_put_contents($dataDir . '/config.json', $json);
    }

    protected function runProcess(array $config): Process
    {
        $this->putConfig($config, $this->dataDir);
        $process = new Process(['php', __DIR__ . '/../../src/run.php', '--data=' . $this->dataDir]);
        $process->run();
        return $process;
    }

    private function getConnection(array $databaseConfig): Connection
    {
        return new Connection($databaseConfig);
    }
}
