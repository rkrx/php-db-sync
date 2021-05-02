<?php
namespace Kir\DBSync\DBDataProvider;

use Kir\DBSync\Common\Json;

class DBQueryFieldSpec {
	/** @var array<string, string> */
	private array $dbFields;
	/** @var array<string, string> */
	private array $fieldMapping;

	/**
	 * @param array<string, string> $dbFields
	 * @param array<string, string> $fieldMapping
	 */
	public function __construct(array $dbFields, array $fieldMapping) {
		$this->dbFields = $dbFields;
		$this->fieldMapping = $fieldMapping;
	}

	/**
	 * @return array<string, string>
	 */
	public function getDBFields(): array {
		return $this->dbFields;
	}

	/**
	 * @param iterable<int, array<string, mixed>> $rows
	 * @return array<string, array{hash: string, keys: array<string, int|float|string>}>
	 */
	public function translateFields($rows): array {
		$result = [];
		foreach($rows as $row) {
			$resultRow = [];
			foreach($this->fieldMapping as $fieldAlias => $fieldName) {
				$resultRow[$fieldName] = $row[$fieldAlias];
			}
			$keyField = Json::encode($resultRow);
			$result[$keyField] = [
				'hash' => $row['v'] ?? null,
				'keys' => $resultRow
			];
		}
		return $result;
	}
}
