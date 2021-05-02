<?php
namespace Kir\DBSync;

use Kir\DBSync\Common\Json;
use Kir\DBSync\DBTable\DBColumn;
use Kir\DBSync\DBTable\DBForeignKey;

class DBTable {
	/** @var string */
	public string $name;
	public object $data;
	/** @var string[] */
	public array $primaryKeyFields = [];
	/** @var DBColumn[] */
	public array $columns = [];
	/** @var DBForeignKey[] */
	public array $foreignKeys = [];

	/**
	 * @param array<'name'|'data'|'columns'|'primaryKeyFields'|'foreignKeys', mixed> $data
	 */
	public function __construct(array $data = []) {
		$this->data = (object) [];
		foreach($data as $key => $value) {
			$this->{$key} = $value;
		}
	}

	/**
	 * @param array<string, mixed> $rowData
	 * @return string
	 */
	public function getPrimaryKeyHash(array $rowData): string {
		$result = [];
		foreach($this->primaryKeyFields as $primaryKeyField) {
			$result[$primaryKeyField] = $rowData[$primaryKeyField];
		}
		return Json::encode($result);
	}

	/**
	 * Returns all fields that are: Not part of the primary key and not generated
	 *
	 * @return string[]
	 */
	public function getNonPrimaryColumnNames(): array {
		$columns = array_filter($this->columns, static fn(DBColumn $col) => !$col->isGenerated);
		$columnNames = array_map(static fn(DBColumn $col) => $col->name, $columns);
		return array_filter($columnNames, fn(string $name) => !in_array($name, $this->primaryKeyFields, true));
	}

	/**
	 * @param array<string, mixed> $dataRow
	 * @return array<string, mixed>
	 */
	public function getOnlyPrimaryKeys(array $dataRow) {
		$result = [];
		foreach($this->primaryKeyFields as $columnName) {
			$result[$columnName] = $dataRow[$columnName] ?? null;
		}
		return $result;
	}

	/**
	 * @param array<string, mixed> $dataRow
	 * @return array<string, mixed>
	 */
	public function getOnlyNonPrimaryKeys(array $dataRow): array {
		$columnNames = $this->getNonPrimaryColumnNames();
		$result = [];
		foreach($columnNames as $columnName) {
			$result[$columnName] = $dataRow[$columnName] ?? null;
		}
		return $result;
	}

	/**
	 * @return string
	 */
	public function __toString() {
		$res = [];
		$res[] = "TABLE {$this->name} " . '{';

		foreach($this->columns as $column) {
			$res[] = "\tCOLUMN {$column->name}";
		}

		if(count($this->primaryKeyFields)) {
			$res[] = sprintf("\tPRIMARY KEY (%s)", implode(', ', $this->primaryKeyFields));
		}

		foreach($this->foreignKeys as $foreignKey) {
			$res[] = sprintf("\tFOREIGN KEY %s.%s (%s) REFERENCES %s.%s (%s)",
				$foreignKey->schema,
				$foreignKey->name,
				implode(', ', $foreignKey->columnNames),
				$foreignKey->primaryTableSchema,
				$foreignKey->primaryTableName,
				implode(', ', $foreignKey->primaryTableColumnNames)
			);
		}

		$res[] = '}';
		return implode("\n", $res);
	}
}
