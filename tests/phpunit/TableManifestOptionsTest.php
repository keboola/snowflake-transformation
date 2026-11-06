<?php

declare(strict_types=1);

namespace Keboola\SnowflakeTransformation\Tests;

use Keboola\SnowflakeTransformation\SnowflakeTransformation;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use PHPUnit\Framework\TestCase;

class TableManifestOptionsTest extends TestCase
{
    private const COLUMN_COMMENTS = [
        'ID' => 'Primary identifier',
        'NAME' => '',
        'CITY' => null,
        'NOTE' => '   ',
    ];

    public function testNewManifestCarriesDescriptions(): void
    {
        $manifest = SnowflakeTransformation::createTableManifestOptions(
            $this->createTableDefinition(),
            self::COLUMN_COMMENTS,
            'Customer table',
            false,
        )->toArray(false);

        $this->assertSame('Customer table', $manifest['description']);
        $this->assertArrayNotHasKey('KBC.description', $manifest['table_metadata']);
        $this->assertSame(
            ['ID' => 'Primary identifier', 'NAME' => null, 'CITY' => null, 'NOTE' => null],
            array_combine(
                array_column($manifest['schema'], 'name'),
                array_map(fn(array $column) => $column['description'] ?? null, $manifest['schema']),
            ),
        );
    }

    public function testLegacyManifestCarriesDescriptionsAsMetadata(): void
    {
        $manifest = SnowflakeTransformation::createTableManifestOptions(
            $this->createTableDefinition(),
            self::COLUMN_COMMENTS,
            'Customer table',
            true,
        )->toArray(true);

        $this->assertArrayNotHasKey('description', $manifest);
        $this->assertContains(['key' => 'KBC.description', 'value' => 'Customer table'], $manifest['metadata']);
        $this->assertContains(
            ['key' => 'KBC.description', 'value' => 'Primary identifier'],
            $manifest['column_metadata']['ID'],
        );
        foreach (['NAME', 'CITY', 'NOTE'] as $column) {
            $this->assertNotContains(
                'KBC.description',
                array_column($manifest['column_metadata'][$column], 'key'),
            );
        }
    }

    /**
     * @dataProvider emptyTableCommentProvider
     */
    public function testEmptyTableCommentIsOmitted(?string $tableComment, bool $legacy): void
    {
        $manifest = SnowflakeTransformation::createTableManifestOptions(
            $this->createTableDefinition(),
            [],
            $tableComment,
            $legacy,
        )->toArray($legacy);

        $this->assertArrayNotHasKey('description', $manifest);
        $tableMetadataKey = $legacy ? 'metadata' : 'table_metadata';
        $this->assertStringNotContainsString('KBC.description', (string) json_encode($manifest[$tableMetadataKey]));
    }

    public function emptyTableCommentProvider(): iterable
    {
        foreach ([true, false] as $legacy) {
            $format = $legacy ? 'legacy' : 'new';
            yield "null, $format" => [null, $legacy];
            yield "empty, $format" => ['', $legacy];
            yield "whitespace, $format" => ['  ', $legacy];
        }
    }

    private function createTableDefinition(): SnowflakeTableDefinition
    {
        $columns = [];
        foreach (array_keys(self::COLUMN_COMMENTS) as $name) {
            $columns[] = SnowflakeColumn::createFromDB([
                'name' => $name,
                'type' => 'VARCHAR(200)',
                'default' => null,
                'null?' => 'Y',
            ]);
        }

        return new SnowflakeTableDefinition('schema', 'customers', false, new ColumnCollection($columns), []);
    }
}
