<?php
namespace Kir\DBSync;

use Generator;
use PDO;

class DBDataService {
	private DBTableProvider $dbTableProvider;

	public function __construct(DBTableProvider $dbTableProvider) {
		$this->dbTableProvider = $dbTableProvider;
	}

	/**
	 * @param string $tableName
	 * @param callable(): Generator<int, array> $dataGeneratorFn
	 */
	public function insertRows(string $tableName, $dataGeneratorFn): void {
		$dataPackets = [];
		$table = $this->dbTableProvider->getTable($tableName);
		$columnNames = array_map(static fn($c) => $c->name, $table->columns);
		$insertStatement = sprintf("INSERT IGNORE INTO %s(%s)VALUES", $tableName, implode(',', $columnNames));
		foreach($dataGeneratorFn() as $row) {
			$insertRow = [];
			foreach($columnNames as $columnName) {
				$insertRow[] = $row[$columnName] ?? null;
			}
			$dataLine = sprintf('(%s)', implode(',', $insertRow));
		}
		$insertStatement .= '';
	}

//	private function flushInsert() {
//	}
}
