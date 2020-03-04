<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeTransformation\Exception\DeadConnectionException;
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

        if ($config->getQueryTimeout()) {
            $sessionVariables['STATEMENT_TIMEOUT_IN_SECONDS'] = $config->getQueryTimeout();
        }

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
        $this->runRetryableQuery($query, 'alter session');
    }

    public function processSteps(array $steps): void
    {
        foreach ($steps as $step) {
            foreach ($step['blocks'] as $block) {
                $this->logger->info(sprintf('Processing block "%s".', $block['name']));
                $this->executionQueries($block['name'], $block['script']);
            }
        }
    }

    public function executionQueries(string $name, array $queries): void
    {
        foreach ($queries as $query) {
            $this->logger->info(sprintf('Running query "%s".', $query));
            $this->runRetryableQuery($query, $name);
        }
    }

    private function createConnection(): Connection
    {
        $connection = new Connection($this->databaseConfig);
        return $connection;
    }

    private function runRetryableQuery(string $query, string $errorMessage): void
    {
        $retryPolicy = new SimpleRetryPolicy(
            SimpleRetryPolicy::DEFAULT_MAX_ATTEMPTS,
            ['PDOException', 'ErrorException']
        );
        $backoffPolicy = new ExponentialBackOffPolicy();
        $retryProxy = new RetryProxy($retryPolicy, $backoffPolicy);

        try {
            $retryProxy->call(function () use ($query): void {
                try {
                    $this->connection->query($query);
                } catch (\Throwable $exception) {
                    $this->tryReconnect();
                    throw $exception;
                }
            });
        } catch (\Throwable $exception) {
            $message = sprintf(
                'Query "%s" in "%s" failed with error: "%s"',
                $this->queryExcerpt($query),
                $errorMessage,
                $exception->getMessage()
            );
            throw new UserException($message, 0, $exception);
        }
    }

    private function tryReconnect(): void
    {
        try {
            $this->testConnection();
        } catch (DeadConnectionException $deadConnectionException) {
            $this->connection = $this->createConnection();
        }
    }

    private function testConnection(): void
    {
        try {
            $this->connection->query('SELECT 1;');
        } catch (\Throwable $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }

    private function queryExcerpt(string $query): string
    {
        if (strlen($query) > 1000) {
            return mb_substr($query, 0, 500, 'UTF-8') . "\n...\n" . mb_substr($query, -500, null, 'UTF-8');
        }
        return $query;
    }
}
