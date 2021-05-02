<?php
namespace Kir\DBSync;

use Kir\DBSync\DBTable\DBColumn;
use Kir\DBSync\DBTable\DBForeignKey;

interface DBTableProvider {
	/**
	 * @return null|string
	 */
	public function getCurrentDatabaseName(): ?string;

	/**
	 * @return string[]
	 */
	public function getAllTableNames(): array;

	/**
	 * @return DBTable[]
	 */
	public function getAllTables();

	/**
	 * @param string $name
	 * @return DBTable
	 */
	public function getTable(string $name): DBTable;

	/**
	 * @param string $tableName
	 * @return array<int, DBColumn>
	 */
	public function getColumns(string $tableName): array;

	/**
	 * @param string $tableName
	 * @return DBForeignKey[]
	 */
	public function getForeignKeys(string $tableName): array;
}
