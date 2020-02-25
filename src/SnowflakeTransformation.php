<?php

declare(strict_types=1);

namespace SnowflakeTransformation;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
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

    public function __construct(Config $config, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->connection = new Connection($config->getDatabaseConfig());
        try {
            if (!empty($config->getDatabaseSchema())) {
                $this->connection->query(sprintf('USE SCHEMA %s', QueryBuilder::quoteIdentifier($config->getDatabaseSchema())));
            }
        } catch (CannotAccessObjectException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    public function setSession(Config $config)
    {
        $sessionVariables = [];
        $sessionVariables['QUERY_TAG'] = sprintf("'%s'", json_encode(['runId' => $config->getRunId()]));

        if ($config->getQueryTimeout()) {
            $sessionVariables['STATEMENT_TIMEOUT_IN_SECONDS'] = $config->getQueryTimeout();
        }

        array_walk($sessionVariables, function (&$item, $key) {
            $item = vsprintf(
                '%s=%s',
                [
                    $key,
                    $item
                ]
            );
        });

        $query = sprintf('ALTER SESSION SET %s;', implode(',', $sessionVariables));
        $this->runRetriableQuery($query, 'alter session');
    }

    public function processSteps(array $steps): void
    {
        foreach ($steps as $step) {
            if ($step['execution'] === 'parallel') {
                foreach ($step['blocks'] as $block) {
                    $this->executionQueries($block['name'], $block['script']);
                }
            } else {
                foreach ($step['blocks'] as $block) {
                    $this->executionQueries($block['name'], $block['script']);
                }
            }
        }
    }

    public function executionQueries(string $name, array $queries): void
    {
        foreach ($queries as $query) {
            $this->logger->info(sprintf('Run query: %s', $query));
            $this->runRetriableQuery($query, $name);
        }
    }

    private function runRetriableQuery(string $query, string $errorMessage)
    {
        $retryPolicy = new SimpleRetryPolicy(
            SimpleRetryPolicy::DEFAULT_MAX_ATTEMPTS,
            ['PDOException', 'ErrorException']
        );
        $backoffPolicy = new ExponentialBackOffPolicy();
        $retryProxy = new RetryProxy($retryPolicy, $backoffPolicy);

        $retryProxy->call(function () use ($query, $errorMessage): void {
            try {
                $this->connection->query($query);
            } catch (\Throwable $e) {
                throw $e;
            }
        });
    }



}
