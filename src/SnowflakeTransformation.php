<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class SnowflakeTransformation
{
    private Connection $connection;

    private LoggerInterface $logger;

    private array $databaseConfig;

    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->databaseConfig = $config->getDatabaseConfig();
        $this->connection = $this->createConnection();
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

    public function processSteps(array $steps): void
    {
        foreach ($steps as $step) {
            $this->logger->info(sprintf('Processing step "%s".', $step['name']));
            $this->processBlocks($step['blocks']);
        }
    }

    public function processBlocks(array $blocks): void
    {
        foreach ($blocks as $block) {
            $this->logger->info(sprintf('Processing block "%s".', $block['name']));
            $this->executionQueries($block['name'], $block['script']);
        }
    }

    public function executionQueries(string $blockName, array $queries): void
    {
        foreach ($queries as $query) {
            $this->logger->info(sprintf('Running query "%s".', $query));

            $uncommentedQuery = \SqlFormatter::removeComments($query);

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
        }
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
