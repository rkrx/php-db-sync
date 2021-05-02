<?php
namespace Kir\DBSync\DBEngines;

use Kir\DBSync\DBEngines\MariaDBEngine\MariaDBTableProvider;
use Kir\DBSync\DBTable;
use Kir\DBSync\DBTableProvider;
use Kir\DBSync\PDOWrapper;
use Kir\DBSync\DBDataProvider;
use PDO;

interface DBEngine {
	/**
	 * @return PDO
	 */
	public function getPDO(): PDO;

	/**
	 * @return PDOWrapper
	 */
	public function getDB(): PDOWrapper;

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
	 * @param DBTable $table
	 * @param array<string, null|int|float|string> $row
	 * @return mixed
	 */
	public function insertRow(DBTable $table, array $row);

	/**
	 * @param DBTable $table
	 * @param array<string, null|int|float|string> $row
	 * @return mixed
	 */
	public function deleteRow(DBTable $table, array $row);
}
