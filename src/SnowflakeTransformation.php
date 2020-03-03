<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\SnowflakeTransformation\Exception\DeadConnectionException;
use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class SnowflakeTransformation
{
    /** @var Connection $connection */
    private $connection;

    /** @var LoggerInterface $logger */
    private $logger;

    /** @var array $databaseConfig */
    private $databaseConfig;

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
                $this->logger->info(sprintf('Start process block "%s"', $block['name']));
                $this->executionQueries($block['name'], $block['script']);
            }
        }
    }

    public function executionQueries(string $name, array $queries): void
    {
        foreach ($queries as $query) {
            $this->logger->info(sprintf('Run query: %s', $query));
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
            $message = sprintf('[%s] Query failed: "%s"', $errorMessage, $query);
            throw new UserException($message, 0, $exception);
        }
    }

    private function tryReconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $deadConnectionException) {
            $this->connection = $this->createConnection();
        }
    }

    private function testConnection(): void
    {
        $this->connection->query('SELECT 1;');
    }

    protected function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (\Throwable $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }
}
