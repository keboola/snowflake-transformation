<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\UserException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\GenericStorage as GenericDatatype;
use Keboola\Datatype\Definition\Snowflake as SnowflakeDatatype;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Psr\Log\LoggerInterface;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;

class SnowflakeTransformation
{
    private Connection $connection;

    private LoggerInterface $logger;

    private Config $config;

    private array $databaseConfig;

    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->config = $config;
        $this->databaseConfig = $config->getDatabaseConfig();
        $this->connection = $this->createConnection();
    }

    public function createManifestMetadata(array $tables, ManifestManager $manifestManager): void
    {
        $getTables = $this->getTables($tables);
        foreach ($getTables as $getTable) {
            $tableManifestOptions = new OutTableManifestOptions();
            $tableName = $getTable['name'];
            $outputMappingTable = array_filter($tables, function ($item) use ($tableName) {
                if ($item['source'] !== $tableName) {
                    return false;
                }
                return true;
            });
            $outputMappingTable = array_values($outputMappingTable);
            $tableManifestOptions->setDestination($outputMappingTable[0]['destination']);

            $columnsMetadata = [];
            foreach ($getTable['columns'] as $column) {
                $datatypeKeys = ['length', 'nullable', 'default'];
                try {
                    $datatype = new SnowflakeDatatype(
                        $column['type'],
                        array_intersect_key($column, array_flip($datatypeKeys))
                    );
                } catch (InvalidTypeException $e) {
                    $datatype = new GenericDatatype(
                        $column['type'],
                        array_intersect_key($column, array_flip($datatypeKeys))
                    );
                }
                $columnMetadata = $datatype->toMetadata();
                $nonDatatypeKeys = array_diff_key($column, array_flip($datatypeKeys));
                foreach ($nonDatatypeKeys as $key => $value) {
                    if ($key !== 'name') {
                        $columnMetadata[] = [
                            'key' => 'KBC.' . $key,
                            'value'=> $value,
                        ];
                    }
                }
                $columnsMetadata[$column['name']] = $columnMetadata;
            }
            unset($getTable['columns']);
            $manifestMetadata = [];
            foreach ($getTable as $key => $value) {
                $manifestMetadata[] = [
                    'key' => 'KBC.' . $key,
                    'value' => $value,
                ];
            }
            $tableManifestOptions
                ->setMetadata($manifestMetadata)
                ->setColumns(array_keys($columnsMetadata))
                ->setColumnMetadata($columnsMetadata)
            ;
            $manifestManager->writeTableManifest($outputMappingTable[0]['destination'], $tableManifestOptions);
        }
    }

    public function setSession(Config $config): void
    {
        $sessionVariables = [];
        $sessionVariables['QUERY_TAG'] = sprintf("'%s'", json_encode(['runId' => getenv('KBC_RUNID')]));
        $sessionVariables['STATEMENT_TIMEOUT_IN_SECONDS'] = $config->getQueryTimeout();

        array_walk($sessionVariables, function (&$item, $key): void {
            $item = vsprintf(
                '%s=%s',
                [
                    $key,
                    $item,
                ]
            );
        });

        $query = sprintf('ALTER SESSION SET %s;', implode(',', $sessionVariables));
        $this->executionQueries('alter session', [$query]);
    }

    public function processBlocks(array $blocks): void
    {
        foreach ($blocks as $block) {
            $this->logger->info(sprintf('Processing block "%s".', $block['name']));
            $this->processCodes($block['codes']);
        }
    }

    public function processCodes(array $codes): void
    {
        foreach ($codes as $code) {
            $this->logger->info(sprintf('Processing codes "%s".', $code['name']));
            $this->executionQueries($code['name'], $code['script']);
        }
    }

    public function executionQueries(string $blockName, array $queries): void
    {
        foreach ($queries as $query) {
            $this->logger->info(sprintf('Running query "%s".', $query));

            $uncommentedQuery = \SqlFormatter::removeComments($query);

            // Do not execute empty queries
            if (strlen(trim($uncommentedQuery)) === 0) {
                continue;
            }

            if (strtoupper(substr($uncommentedQuery, 0, 6)) === 'SELECT') {
                continue;
            }

            try {
                $this->connection->query($uncommentedQuery);
            } catch (\Throwable $exception) {
                $message = sprintf(
                    'Query "%s" in "%s" failed with error: "%s"',
                    $this->queryExcerpt($query),
                    $blockName,
                    $exception->getMessage()
                );
                throw new UserException($message, 0, $exception);
            }

            $this->checkUserTermination();
        }
    }

    protected function checkUserTermination(): void
    {
        $result = $this->connection->fetchAll("SHOW VARIABLES LIKE 'ABORT_TRANSFORMATION'");

        if (count($result) === 0) {
            return;
        }

        if ($result[0]['value'] !== '') {
            throw new UserException(
                sprintf('Transformation aborted with message "%s"', $result[0]['value'])
            );
        }
    }

    private function getTables(array $tables): array
    {
        if (count($tables) === 0) {
            return [];
        }
        $sourceTables = array_map(function ($item) {
            return $item['source'];
        }, $tables);

        $columnSql = sprintf(
            'SELECT * FROM %s WHERE TABLE_NAME IN (%s)',
            'information_schema.columns',
            implode(', ', array_map(function ($item) {
                return QueryBuilder::quote($item);
            }, $sourceTables))
        );
        $columns = $this->connection->fetchAll($columnSql);

        $tableDefs = [];
        foreach ($columns as $column) {
            if (!isset($tableDefs[$column['TABLE_NAME']])) {
                $tableDefs[$column['TABLE_NAME']] = [
                    'name' => $column['TABLE_NAME'],
                    'columns' => [],
                ];
            }

            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if (is_numeric($column['NUMERIC_SCALE'])) {
                    $length = $column['NUMERIC_PRECISION'] . ',' . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }

            $tableDefs[$column['TABLE_NAME']]['columns'][] = [
                'name' => $column['COLUMN_NAME'],
                'default' => $column['COLUMN_DEFAULT'],
                'length' => $length,
                'nullable' => (trim($column['IS_NULLABLE']) === 'NO') ? false : true,
                'type' => $column['DATA_TYPE'],
            ];
        }

        if (count($tableDefs) !== count($sourceTables)) {
            $missingTables = array_diff(
                $sourceTables,
                array_map(function (array $item): string {
                    return $item['name'];
                }, $tableDefs)
            );
            throw new UserException(sprintf('Missing create tables "%s"', implode('", "', $missingTables)));
        }
        return $tableDefs;
    }

    private function createConnection(): Connection
    {
        $connection = new Connection($this->databaseConfig);
        return $connection;
    }

    private function queryExcerpt(string $query): string
    {
        if (strlen($query) > 1000) {
            return mb_substr($query, 0, 500, 'UTF-8') . "\n...\n" . mb_substr($query, -500, null, 'UTF-8');
        }
        return $query;
    }
}
