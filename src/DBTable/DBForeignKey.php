<?php
namespace Kir\DBSync\DBTable;

class DBForeignKey {
	public string $schema;
	public string $name;

	public string $tableSchema;
	public string $tableName;
	/** @var string[] */
	public array $columnNames;

	public string $primaryTableSchema;
	public string $primaryTableName;

	/** @var string[] */
	public array $primaryTableColumnNames;

	/**
	 * @param array{schema?: string, name?: string, tableSchema?: string, tableName?: string, columnNames?: string[], primaryTableSchema?: string, primaryTableName?: string, primaryTableColumnNames?: string[]} $data
	 */
	public function __construct(array $data = []) {
		foreach($data as $key => $value) {
			$this->{$key} = $value;
		}
	}

	public function __toString() {
		return sprintf("FOREIGN KEY %s.%s TABLE %s.%s (%s) REFERENCES %s.%s (%s)",
			$this->schema,
			$this->name,
			$this->tableSchema,
			$this->tableName,
			implode(', ', $this->columnNames),
			$this->primaryTableSchema,
			$this->primaryTableName,
			implode(', ', $this->primaryTableColumnNames)
		);
	}
}
