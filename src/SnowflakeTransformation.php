<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;
use Keboola\Component\UserException;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\Snowflake as SnowflakeDatatype;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Psr\Log\LoggerInterface;
use SqlFormatter;
use Throwable;

class SnowflakeTransformation
{
    private const ABORT_TRANSFORMATION = 'ABORT_TRANSFORMATION';

    private Connection $connection;

    private LoggerInterface $logger;

    private array $databaseConfig;

    /**
     * @throws \Keboola\SnowflakeTransformation\Exception\ApplicationException
     * @throws \Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException
     */
    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->databaseConfig = $config->getDatabaseConfig();
        $this->connection = new Connection($this->databaseConfig);
    }

    /**
     * @throws \Keboola\Component\Manifest\ManifestManager\Options\OptionsValidationException
     * @throws \Keboola\Component\UserException
     * @param array<array{source: string, write_always?: bool}> $tableNames
     * @param ManifestManager $manifestManager
     */
    public function createManifestMetadata(
        array $tableNames,
        ManifestManager $manifestManager,
        bool $transformationFailed = false
    ): void {
        $tableStructures = $this->getTables($tableNames, $transformationFailed);
        foreach ($tableStructures as $tableDef) {
            $columnsMetadata = (object) [];
            /** @var SnowflakeColumn $column */
            foreach ($tableDef->getColumnsDefinitions() as $column) {
                $columnsMetadata->{$column->getColumnName()} = $column->getColumnDefinition()->toMetadata();
            }
            $tableMetadata = [];
            $tableMetadata[] = [
                'key' => 'KBC.name',
                'value' => $tableDef->getTableName(),
            ];
            // add metadata indicating that this output is snowflake native
            $tableMetadata[] = [
                'key' => Common::KBC_METADATA_KEY_BACKEND,
                'value' => SnowflakeDatatype::METADATA_BACKEND,
            ];

            $tableManifestOptions = new OutTableManifestOptions();
            $tableManifestOptions
                ->setMetadata($tableMetadata)
                ->setColumns($tableDef->getColumnsNames())
                ->setColumnMetadata($columnsMetadata)
            ;
            $manifestManager->writeTableManifest($tableDef->getTableName(), $tableManifestOptions);
        }
    }

    /**
     * @throws \Keboola\Component\UserException
     */
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

    /**
     * @throws \Keboola\Component\UserException
     */
    public function processBlocks(array $blocks): void
    {
        foreach ($blocks as $block) {
            $this->logger->info(sprintf('Processing block "%s".', $block['name']));
            $this->processCodes($block['codes']);
        }
    }

    /**
     * @throws \Keboola\Component\UserException
     */
    public function processCodes(array $codes): void
    {
        foreach ($codes as $code) {
            $this->logger->info(sprintf('Processing code "%s".', $code['name']));
            $this->executeQueries($code['name'], $code['script']);
        }
    }

    /**
     * @throws \Keboola\Component\UserException
     */
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
            } catch (Throwable $exception) {
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

    /**
     * @throws \Keboola\Component\UserException
     */
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

    /**
     * @throws \Keboola\Component\UserException
     * @param array<array{source: string, write_always?: bool}> $tables
     * @return SnowflakeTableDefinition[]
     */
    private function getTables(array $tables, bool $transformationFailed): array
    {
        if (count($tables) === 0) {
            return [];
        }

        if ($transformationFailed) {
            $tables = array_filter($tables, function ($item) {
                return isset($item['write_always']) && $item['write_always'] === true;
            });
        }

        $sourceTables = array_column($tables, 'source');

        $defs = [];
        $schema = $this->databaseConfig['schema'];
        $missingTables = [];
        foreach ($sourceTables as $tableName) {
            try {
                /** @var array<array{
                 *     name: string,
                 *     kind: string,
                 *     type: string,
                 *     default: string,
                 *     'null?': string
                 * }> $columnsMeta */
                $columnsMeta = $this->connection->fetchAll((
                sprintf(
                    'DESC TABLE %s',
                    SnowflakeQuote::createQuotedIdentifierFromParts([$schema, $tableName,])
                )
                ));
            } catch (RuntimeException $e) {
                $missingTables[] = $tableName;
                continue;
            }

            $columns = [];

            foreach ($columnsMeta as $col) {
                if ($col['kind'] === 'COLUMN') {
                    $columns[] = SnowflakeColumn::createFromDB($col);
                }
            }

            $defs[] = new SnowflakeTableDefinition(
                $schema,
                $tableName,
                false,
                new ColumnCollection($columns),
                []
            );
        }

        if ($missingTables) {
            throw new UserException(
                sprintf(
                    'Tables "%s" specified in output were not created by the transformation.',
                    implode('", "', $missingTables)
                )
            );
        }
        return $defs;
    }

    private function queryExcerpt(string $query): string
    {
        if (mb_strlen($query) > 1000) {
            return mb_substr($query, 0, 500, 'UTF-8') . "\n...\n" . mb_substr($query, -500, null, 'UTF-8');
        }
        return $query;
    }

    /**
     * @throws \Keboola\Component\UserException
     */
    public function setKbcEnvVars(): void
    {
        $kbcEnvVars = [
            'KBC_RUNID',
            'KBC_PROJECTID',
            'KBC_STACKID',
            'KBC_CONFIGID',
            'KBC_COMPONENTID',
            'KBC_CONFIGROWID',
            'KBC_BRANCHID',
        ];

        $variables = [];
        foreach ($kbcEnvVars as $kbcEnvVar) {
            $value = getenv($kbcEnvVar);
            if ($value) {
                $variables[$kbcEnvVar] = sprintf("'%s'", $value);
            }
        }

        $query = sprintf(
            'SET (%s) = (%s);',
            implode(', ', array_keys($variables)),
            implode(', ', array_values($variables))
        );

        $this->executeQueries('set variables', [$query]);
    }
}
