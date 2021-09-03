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
use SqlFormatter;

class SnowflakeTransformation
{
    private const ABORT_TRANSFORMATION = 'ABORT_TRANSFORMATION';

    private Connection $connection;

    private LoggerInterface $logger;

    private Config $config;

    private array $databaseConfig;

    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->config = $config;
        $this->databaseConfig = $config->getDatabaseConfig();
        $this->connection = new Connection($this->databaseConfig);
    }

    public function createManifestMetadata(array $tableNames, ManifestManager $manifestManager): void
    {
        $tableStructures = $this->getTables($tableNames);
        foreach ($tableStructures as $tableStructure) {
            $columnsMetadata = (object) [];
            foreach ($tableStructure['columns'] as $column) {
                $datatypeKeys = array_flip(['length', 'nullable']);
                try {
                    $datatype = new SnowflakeDatatype(
                        $column['type'],
                        array_intersect_key($column, $datatypeKeys)
                    );
                } catch (InvalidTypeException $e) {
                    unset($column['length']);
                    $datatype = new GenericDatatype(
                        $column['type'],
                        array_intersect_key($column, $datatypeKeys)
                    );
                }
                $columnsMetadata->{$column['name']} = $datatype->toMetadata();
            }
            unset($tableStructure['columns']);
            $tableMetadata = [];
            foreach ($tableStructure as $key => $value) {
                $tableMetadata[] = [
                    'key' => 'KBC.' . $key,
                    'value' => $value,
                ];
            }

            $tableManifestOptions = new OutTableManifestOptions();
            $tableManifestOptions
                ->setMetadata($tableMetadata)
                ->setColumnMetadata($columnsMetadata)
            ;
            $manifestManager->writeTableManifest($tableStructure['name'], $tableManifestOptions);
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
        $this->executeQueries('alter session', [$query]);
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
            $this->logger->info(sprintf('Processing code "%s".', $code['name']));
            $this->executeQueries($code['name'], $code['script']);
        }
    }

    public function executeQueries(string $blockName, array $queries): void
    {
        foreach ($queries as $query) {
            $uncommentedQuery = SqlFormatter::removeComments($query, false);

            // Do not execute empty queries
            if (strlen(trim($uncommentedQuery)) === 0) {
                continue;
            }

            if (strtoupper(substr($uncommentedQuery, 0, 6)) === 'SELECT') {
                continue;
            }

            $this->logger->info(sprintf('Running query "%s".', $this->queryExcerpt($query)));
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

            $pattern = sprintf('/%s/i', preg_quote(self::ABORT_TRANSFORMATION, '/'));
            if (preg_match($pattern, $uncommentedQuery)) {
                $this->checkUserTermination();
            }
        }
    }

    protected function checkUserTermination(): void
    {
        $this->logger->info('Checking user termination');
        $result = $this->connection->fetchAll(
            sprintf(
                'SHOW VARIABLES LIKE %s',
                QueryBuilder::quote(self::ABORT_TRANSFORMATION)
            )
        );

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

        $nameColumns = [
            'TABLE_NAME',
            'COLUMN_NAME',
            'CHARACTER_MAXIMUM_LENGTH',
            'NUMERIC_PRECISION',
            'NUMERIC_SCALE',
            'IS_NULLABLE',
            'DATA_TYPE',
        ];
        $columnSql = sprintf(
            'SELECT %s FROM %s WHERE TABLE_NAME IN (%s)',
            implode(', ', array_map(function ($item) {
                return QueryBuilder::quoteIdentifier($item);
            }, $nameColumns)),
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

            $tableDefs[$column['TABLE_NAME']]['columns'][] = [
                'name' => $column['COLUMN_NAME'],
                'length' => [
                    'character_maximum' => $column['CHARACTER_MAXIMUM_LENGTH'],
                    'numeric_precision' => $column['NUMERIC_PRECISION'],
                    'numeric_scale' => $column['NUMERIC_SCALE'],
                ],
                'nullable' => (trim($column['IS_NULLABLE']) === 'NO') ? false : true,
                'type' => $column['DATA_TYPE'],
            ];
        }

        $missingTables = array_diff($sourceTables, array_keys($tableDefs));
        if ($missingTables) {
            throw new UserException(
                sprintf(
                    'Tables "%s" specified in output were not created by the transformation.',
                    implode('", "', $missingTables)
                )
            );
        }
        return $tableDefs;
    }

    private function queryExcerpt(string $query): string
    {
        if (mb_strlen($query) > 1000) {
            return mb_substr($query, 0, 500, 'UTF-8') . "\n...\n" . mb_substr($query, -500, null, 'UTF-8');
        }
        return $query;
    }
}
