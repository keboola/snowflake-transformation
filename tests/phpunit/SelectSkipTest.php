<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\Tests;

use Keboola\SnowflakeTransformation\SnowflakeTransformation;
use PHPUnit\Framework\TestCase;
use SqlFormatter;

class SelectSkipTest extends TestCase
{
    /**
     * @dataProvider skippedQueryProvider
     */
    public function testSelectIsSkippedRegardlessOfLeadingWhitespace(string $query): void
    {
        // Same comment removal as SnowflakeTransformation::executeQueries()
        $uncommentedQuery = SqlFormatter::removeComments($query, false);

        self::assertTrue(SnowflakeTransformation::isSkippedSelect($uncommentedQuery));
    }

    /**
     * @dataProvider executedQueryProvider
     */
    public function testOtherStatementsAreExecuted(string $query): void
    {
        $uncommentedQuery = SqlFormatter::removeComments($query, false);

        self::assertFalse(SnowflakeTransformation::isSkippedSelect($uncommentedQuery));
    }

    /**
     * @return array<string, array{string}>
     */
    public function skippedQueryProvider(): array
    {
        return [
            'select' => ['SELECT * FROM "missing_table"'],
            'lowercase select' => ['select 1'],
            'leading spaces' => ['  SELECT * FROM "missing_table"'],
            'leading tab' => ["\tSELECT 1"],
            'leading newline' => ["\nSELECT * FROM \"missing_table\""],
            'leading CRLF' => ["\r\nSELECT 1"],
            'line comment before' => ["-- comment\nSELECT * FROM \"missing_table\""],
            'block comment before' => ['/* comment */ SELECT * FROM "missing_table"'],
            'select followed by newline' => ["SELECT\n*\nFROM \"missing_table\""],
            'select star without space' => ['SELECT* FROM "missing_table"'],
        ];
    }

    /**
     * @return array<string, array{string}>
     */
    public function executedQueryProvider(): array
    {
        return [
            'with select' => ['WITH "x" AS (SELECT 1 AS "id") SELECT * FROM "x"'],
            'create table as select' => ['CREATE TABLE "output" AS SELECT * FROM "input"'],
            'insert select' => ['INSERT INTO "output" SELECT * FROM "input"'],
            'leading whitespace create' => ["\n  CREATE TABLE \"output\" (\"id\" INT)"],
            'identifier starting with select' => ['SELECTED_ROWS'],
        ];
    }
}
