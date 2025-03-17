<?php
namespace Kir\DBSync\DBEngines\MariaDBEngine;

use Kir\DBSync\Common\Cache;
use Kir\DBSync\PDOWrapper;
use Kir\DBSync\DBTable;
use Kir\DBSync\DBTable\DBColumn;
use Kir\DBSync\DBTable\DBForeignKey;
use Kir\DBSync\DBTableProvider;

/**
 * @phpstan-type TColumnShape array{
 *     COLUMN_NAME: string,
 *     ORDINAL_POSITION: int,
 *     COLUMN_DEFAULT: null|int|float|string,
 *     IS_NULLABLE: string,
 *     DATA_TYPE: string,
 *     NUMERIC_PRECISION: int|null,
 *     NUMERIC_SCALE: int|null,
 *     COLUMN_COMMENT: string,
 *     IS_GENERATED: string|null,
 *     GENERATION_EXPRESSION: string|null
 * }
 */
class MariaDBTableProvider implements DBTableProvider {
	private PDOWrapper $db;
	private Cache $cache;

	public function __construct(PDOWrapper $db) {
		$this->db = $db;
		$this->cache = new Cache();
	}

	/**
	 * @return null|string
	 */
	public function getCurrentDatabaseName(): ?string {
		return $this->cache->getOr(__FUNCTION__, function () {
			return $this->db->fetchString('SELECT DATABASE()');
		});
	}

	/**
	 * @return string[]
	 */
	public function getAllTableNames(): array {
		return $this->cache->getOr(__FUNCTION__, function () {
			$database = $this->getCurrentDatabaseName();
			$query = "SHOW FULL TABLES FROM `{$database}` WHERE TABLE_TYPE = 'BASE TABLE'";
			return $this->db->fetchArrayCallback($query, [], 'strval');
		});
	}

	/**
	 * @return DBTable[]
	 */
	public function getAllTables() {
		$tableNames = $this->getAllTableNames();
		return array_map(fn($tableName) => $this->getTable($tableName), $tableNames);
	}

	/**
	 * @param string $name
	 * @return DBTable
	 */
	public function getTable(string $name): DBTable {
		return new DBTable([
			'name' => $name,
			'data' => (object) [],
			'columns' => $this->getColumns($name),
			'primaryKeyFields' => $this->getPrimaryKeyFields($name),
			'foreignKeys' => $this->getForeignKeys($name)
		]);
	}

	/**
	 * @param string $tableName
	 * @return array<int, DBColumn>
	 */
	public function getColumns(string $tableName): array {
		$allColumns = $this->cache->getOr(__FUNCTION__, function () {
			$query = "
				SELECT
					t.TABLE_NAME,
					t.COLUMN_NAME,
					t.ORDINAL_POSITION,
					t.COLUMN_DEFAULT,
					t.IS_NULLABLE,
					t.DATA_TYPE,
					t.CHARACTER_MAXIMUM_LENGTH,
					t.CHARACTER_OCTET_LENGTH,
					t.NUMERIC_PRECISION,
					t.NUMERIC_SCALE,
					t.DATETIME_PRECISION,
					t.CHARACTER_SET_NAME,
					t.COLLATION_NAME,
					t.COLUMN_TYPE,
					t.COLUMN_KEY,
					t.EXTRA,
					t.PRIVILEGES,
					t.COLUMN_COMMENT,
					t.IS_GENERATED,
					t.GENERATION_EXPRESSION
				FROM
					information_schema.COLUMNS t
				WHERE
					t.TABLE_SCHEMA = :db
				ORDER BY
					t.ORDINAL_POSITION
			";
			$params = ['db' => $this->getCurrentDatabaseName()];
			$columns = $this->db->fetchRows($query, $params);
			$result = [];
			foreach($columns as $column) {
				$tmpTableName = $column['TABLE_NAME'];
				unset($column['TABLE_NAME']);
				$result[$tmpTableName] = $result[$tmpTableName] ?? [];
				$result[$tmpTableName][] = $column;
			}
			return $result;
		});
		$mappingFn = static function (array $row) {
			/** @var TColumnShape $row */

			$columnDef = [
				'name' => $row['COLUMN_NAME'],
				'position' => $row['ORDINAL_POSITION'],
				'defaultValue' => $row['COLUMN_DEFAULT'],
				'isNullable' => $row['IS_NULLABLE'] !== 'NO',
				'dataType' => $row['DATA_TYPE'],
				'isGenerated' => ($row['IS_GENERATED'] ?? '') !== 'NEVER',
				'expression' => $row['GENERATION_EXPRESSION'] ?? null,
			];

			if($row['NUMERIC_PRECISION']) {
				$columnDef['numericPrecision'] = $row['NUMERIC_PRECISION'];
			}

			if($row['NUMERIC_SCALE']) {
				$columnDef['numericScale'] = $row['NUMERIC_SCALE'];
			}

			if($row['COLUMN_COMMENT']) {
				$columnDef['comment'] = $row['COLUMN_COMMENT'];
			}

			if($row['COLUMN_COMMENT']) {
				$columnDef['comment'] = $row['COLUMN_COMMENT'];
			}

			return new DBColumn($columnDef);
		};
		return array_map($mappingFn, $allColumns[$tableName] ?? []);
	}

	/**
	 * @return array{TABLE_NAME?: string, COLUMN_NAME?: string} The field names in their original order in the primary key
	 */
	private function getPrimaryKeyFields(string $tableName): array {
		$allPrimaryKeys = $this->cache->getOr(__FUNCTION__, function () {
			$query = "
				SELECT
					kcu.TABLE_NAME,
					kcu.COLUMN_NAME
				FROM
					information_schema.key_column_usage kcu
				INNER JOIN
				    information_schema.tables tab ON kcu.table_schema = tab.table_schema AND kcu.table_name = tab.table_name
				WHERE
					kcu.TABLE_SCHEMA = :db
					AND
					tab.TABLE_TYPE = 'BASE TABLE'
					AND
					ISNULL(kcu.REFERENCED_TABLE_NAME)
					AND
					kcu.CONSTRAINT_NAME = 'PRIMARY'
				ORDER BY
					kcu.ORDINAL_POSITION;
			";
			$params = ['db' => $this->getCurrentDatabaseName()];
			$rows = $this->db->fetchRows($query, $params);
			$result = [];
			foreach($rows as $row) {
				$result[$row['TABLE_NAME']] = $result[$row['TABLE_NAME']] ?? [];
				$result[$row['TABLE_NAME']][] = $row['COLUMN_NAME'];
			}
			return $result;
		});
		return $allPrimaryKeys[$tableName] ?? [];
	}

	/**
	 * @param string $tableName
	 * @return DBForeignKey[]
	 */
	public function getForeignKeys(string $tableName): array {
		$allForeignKeys = $this->cache->getOr(__FUNCTION__, function () {
			$query = "
				SELECT
					kcu.TABLE_NAME AS 'table_name',
					kcu.CONSTRAINT_SCHEMA AS 'constraint_schema',
					kcu.CONSTRAINT_NAME AS 'constraint_name',
					kcu.TABLE_SCHEMA AS 'schema',
					kcu.TABLE_NAME AS 'table_name',
					GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION SEPARATOR ',') AS 'column_names',
					kcu.REFERENCED_TABLE_SCHEMA AS 'referenced_table_schema',
					kcu.REFERENCED_TABLE_NAME AS 'referenced_table_name',
					GROUP_CONCAT(kcu.REFERENCED_COLUMN_NAME ORDER BY kcu.POSITION_IN_UNIQUE_CONSTRAINT SEPARATOR ',') AS 'referenced_column_names'
				FROM
					information_schema.key_column_usage kcu
				INNER JOIN
					information_schema.tables tab ON kcu.table_schema = tab.table_schema AND kcu.table_name = tab.table_name
				WHERE
					kcu.TABLE_SCHEMA = :db
					AND
					tab.TABLE_TYPE = 'BASE TABLE'
					AND
					NOT ISNULL(kcu.REFERENCED_TABLE_NAME)
				GROUP BY
					kcu.CONSTRAINT_SCHEMA,
					kcu.CONSTRAINT_NAME,
					kcu.TABLE_SCHEMA,
					kcu.TABLE_NAME,
					kcu.REFERENCED_TABLE_SCHEMA,
					kcu.REFERENCED_TABLE_NAME
				ORDER BY
					kcu.CONSTRAINT_SCHEMA,
					kcu.CONSTRAINT_NAME,
					kcu.TABLE_SCHEMA,
					kcu.TABLE_NAME,
					kcu.REFERENCED_TABLE_SCHEMA,
					kcu.REFERENCED_TABLE_NAME
				;
			";
			$allForeignKeys = [];
			$params = ['db' => $this->getCurrentDatabaseName()];
			$rows = $this->db->fetchRows($query, $params);
			foreach($rows as $row) {
				$tableName = $row['table_name'];
				$allForeignKeys[$tableName] = $allForeignKeys[$tableName] ?? [];
				$allForeignKeys[$tableName][] = $row;
			}
			return $allForeignKeys;
		});
		$result = [];
		/** @var array{constraint_schema: string, constraint_name: string, schema: string, table_name: string, column_names: string, referenced_table_schema: string, referenced_table_name: string, referenced_column_names: string} $row */
		foreach ($allForeignKeys[$tableName] ?? [] as $row) {
			$result[] = new DBForeignKey([
				'schema' => $row['constraint_schema'],
				'name' => $row['constraint_name'],
				'tableSchema' => $row['schema'],
				'tableName' => $row['table_name'],
				'columnNames' => explode(',', $row['column_names']),
				'primaryTableSchema' => $row['referenced_table_schema'],
				'primaryTableName' => $row['referenced_table_name'],
				'primaryTableColumnNames' => explode(',', $row['referenced_column_names']),
			]);
		}
		return $result;
	}
}
