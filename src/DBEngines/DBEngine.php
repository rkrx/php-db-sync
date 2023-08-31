<?php
namespace Kir\DBSync\DBEngines;

use Generator;
use Kir\DBSync\Common\AbstractStatement;
use Kir\DBSync\DBTable;
use Kir\DBSync\DBTableProvider;
use Kir\DBSync\DBDataProvider;
use PDO;

interface DBEngine {
	/**
	 * @return PDO
	 */
	public function getPDO(): PDO;

	/**
	 * @return DBTableProvider
	 */
	public function getTableProvider(): DBTableProvider;

	/**
	 * @return DBDataProvider
	 */
	public function getDataProvider(): DBDataProvider;

	/**
	 * @param string $fieldName
	 * @param string|null $alias
	 * @return string
	 */
	public function quoteFieldName(string $fieldName, ?string $alias = null): string;

	/**
	 * @param null|bool|int|float|string $value
	 * @return string
	 */
	public function quoteValue($value): string;

	/**
	 * @return Generator<AbstractStatement>
	 */
	public function setUp(): Generator;

	/**
	 * @return Generator<AbstractStatement>
	 */
	public function tearDown(): Generator;

	/**
	 * @param DBTable $table
	 * @param array<string, null|int|float|string> $row
	 * @return string
	 */
	public function makeInsertStatement(DBTable $table, array $row);

	/**
	 * @param DBTable $table
	 * @param array<string, mixed> $updateValues
	 * @param array<string, mixed> $keys
	 * @return string
	 */
	public function makeUpdateStatement(DBTable $table, array $updateValues, array $keys);

	/**
	 * @param DBTable $table
	 * @param array<string, null|int|float|string> $row
	 * @return string
	 */
	public function makeDeleteStatement(DBTable $table, array $row);
}
