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

    /** @var Client $storageApiClient */
    private $storageApiClient;

    public function setUp(): void
    {
        $databaseConfig = $this->getDatabaseConfig();
        $this->connection = $this->getConnection($databaseConfig);

        $this->connection->query(
            sprintf('USE SCHEMA %s', QueryBuilder::quoteIdentifier($databaseConfig['schema']))
        );

        $this->storageApiClient = new Client([
            'url' => 'https://connection.keboola.com',
            'token' => getenv('STORAGE_API_TOKEN'),
        ]);

        $this->setupTables();
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

    private function setupTables(): void
    {
        $salesCsv = new CsvFile($this->dataDir . '/sales.csv');
        $this->createTextTable($salesCsv, 'sales');

        $categoriesCsv = new CsvFile($this->dataDir . '/categories.csv');
        $this->createTextTable($categoriesCsv, 'categories');
    }

    private function createTextTable(CsvFile $file, string $tableName): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS %s',
            QueryBuilder::quoteIdentifier($tableName)
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE %s (%s);',
            QueryBuilder::quoteIdentifier($tableName),
            implode(
                ', ',
                array_map(function ($column) {
                    $q = '"';
                    return ($q . str_replace("$q", "$q$q", $column) . $q) . ' VARCHAR(200) NOT NULL';
                }, $file->getHeader())
            )
        ));

        $storageFileInfo = $this->storageApiClient->getFile(
            (string) $this->storageApiClient->uploadFile(
                (string) $file,
                new FileUploadOptions()
            ),
            (new GetFileOptions())->setFederationToken(true)
        );

        $sql = $this->generateCreateCommand($tableName, $file, $storageFileInfo);
        $this->connection->query($sql);

        $sql = sprintf(
            'SELECT COUNT(*) AS ROWCOUNT FROM %s',
            QueryBuilder::quoteIdentifier($tableName)
        );
        $result = $this->connection->fetchAll($sql);
        $this->assertEquals($this->countTable($file), (int) $result[0]['ROWCOUNT']);
    }

    private function generateCreateCommand(string $tableName, CsvFile $csv, array $fileInfo): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', QueryBuilder::quoteIdentifier($csv->getDelimiter()));
        $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $this->quote($csv->getEnclosure()));
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', QueryBuilder::quoteIdentifier('\\'));

        if (!$fileInfo['isSliced']) {
            $csvOptions[] = 'SKIP_HEADER = 1';
        }

        return sprintf(
            "
             COPY INTO %s
             FROM 's3://%s/%s'
             FILE_FORMAT = (TYPE=CSV %s)
             CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s  AWS_TOKEN = %s)
            ",
            QueryBuilder::quoteIdentifier($tableName),
            $fileInfo['s3Path']['bucket'],
            $fileInfo['s3Path']['key'],
            implode(' ', $csvOptions),
            $this->quote($fileInfo['credentials']['AccessKeyId']),
            $this->quote($fileInfo['credentials']['SecretAccessKey']),
            $this->quote($fileInfo['credentials']['SessionToken'])
        );
    }

    private function countTable(CsvFile $file): int
    {
        $linesCount = 0;
        foreach ($file as $i => $line) {
            $linesCount++;
        }
        return $linesCount-1; // skip header
    }

    private function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }
}
