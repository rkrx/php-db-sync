<?php
namespace Kir\DBSync\DBEngines\MySQLEngine;

use Generator;
use Kir\DBSync\Common\Json;
use Kir\DBSync\DBDataProvider;
use Kir\DBSync\DBEngines\MySQLEngine;
use Kir\DBSync\DBTable;
use Kir\DBSync\DBOffsetConditionBuilder;
use Kir\MySQL\Builder\RunnableSelect;

class MySQLDataProvider implements DBDataProvider {
	private DBOffsetConditionBuilder $offsetConditionBuilder;
	private MySQLEngine $dbEngine;

	/**
	 * @param MySQLEngine $dbEngine
	 */
	public function __construct(MySQLEngine $dbEngine) {
		$this->dbEngine = $dbEngine;
		$this->offsetConditionBuilder = new DBOffsetConditionBuilder($dbEngine);
	}

	/**
	 * @param string $tableName
	 * @param string[] $keyFields
	 * @param null|array<string, int|float|string> $lowerBound
	 * @param null|array<string, int|float|string> $upperBound
	 * @return array<int, array<string, mixed>>
	 */
	public function getKeysInLowerAndUpperBound(string $tableName, array $keyFields, ?array $lowerBound, ?array $upperBound): array {
		return $this->getBaseSelectWithLowerAndUpperBound($tableName, $keyFields, $lowerBound,$upperBound)
		->setPreserveTypes()
		->fields(array_map(fn($keyField) => $this->dbEngine->quoteFieldName($keyField), $keyFields))
		->fetchRows();
	}

	/**
	 * @param string $tableName
	 * @param array<int, string> $keyFields
	 * @param int $limit
	 * @param null|array<string, int|float|string> $offset
	 * @return null|array<string, int|float|string>
	 */
	public function getGreatestKeyInRange(string $tableName, array $keyFields, int $limit, ?array $offset = null): ?array {
		$fields = [];
		$resultFields = [];
		foreach($keyFields as $key => $fieldName) {
			$fields["k{$key}"] = sprintf('`a`.`%s`', $fieldName);
			$resultFields["k{$key}"] = $fieldName;
		}

		$select = $this->dbEngine->select()
		->fields($fields)
		->from('a USE KEY (PRIMARY)', $tableName)
		->limit($limit);

		$execParams = [];
		if($offset !== null) {
			[$cond, $execParams] = $this->offsetConditionBuilder->buildGreaterThan($keyFields, $offset);
			$select->where($cond);
		}

		foreach($keyFields as $keyField) {
			$select->orderBy(sprintf('`a`.`%s`', $keyField), 'ASC');
		}

		$select->bindValues($execParams);

		$rows = $select->fetchRows();

		if(count($rows)) {
			$rowLastRow = $rows[count($rows) - 1];
			$result = [];
			/**
			 * @var string $key
			 * @var int|float|string $value
			 */
			foreach($rowLastRow as $key => $value) {
				$result[$resultFields[$key]] = $value;
			}
			return $result;
		}

		return null;
	}

	/**
	 * @param string $tableName
	 * @param string[] $keyFields
	 * @param string[] $valueFields
	 * @param iterable<int, array<string, int|float|string>> $equalKeySets
	 * @return array<string, array{hash: string, keys: array<string, int|float|string>}>
	 */
	public function getKeysWithHashedValues(string $tableName, array $keyFields, array $valueFields, iterable $equalKeySets): array {
		$conditionList = $this->buildConditionList($equalKeySets);

		if(!count($conditionList)) {
			return [];
		}

		$dbFields = [];
		$mapping = [];
		foreach(array_values($keyFields) as $idx => $field) {
			$fieldKey = "k{$idx}";
			$dbFields[$fieldKey] = $this->dbEngine->quoteFieldName($field, 'a');
			$mapping[$field] = $fieldKey;
		}

		$params = [];
		foreach($valueFields as $fieldName) {
			$params[] = $this->dbEngine->quoteValue($fieldName);
			$params[] = $this->dbEngine->quoteFieldName($fieldName, 'a');
		}

		$hash = sprintf('MD5(JSON_OBJECT(%s))', implode(', ', $params));
		$dbFields['v'] = $hash;

		$select = $this->dbEngine->select()
		->fields($dbFields)
		->from('a USE KEY (PRIMARY)', $tableName)
		->where(implode("\n\tOR\n", $conditionList));

		$select->setPreserveTypes();

		$result = [];
		/** @var array<string, int|float|string> $row */
		foreach($select->fetchRows() as $row) {
			$keyValues = [];
			foreach($keyFields as $keyField) {
				$keyValues[$keyField] = $row[$mapping[$keyField]];
			}
			$key = Json::encode($keyValues);
			$result[$key] = [
				'hash' => (string) $row['v'],
				'keys' => $keyValues
			];
		}
		return $result;
	}

	/**
	 * @param DBTable $table
	 * @param iterable<int, array<string, int|float|string>> $keySets
	 * @return Generator<string, array<string, int|float|string>>
	 */
	public function getKeyAndValueColumnsLazy(DBTable $table, iterable $keySets): Generator {
		$conditionList = $this->buildConditionList($keySets);

		if(!count($conditionList)) {
			return [];
		}

		$mapping = [];
		$fields = [];
		foreach(array_values($table->columns) as $idx => $column) {
			$fields["f{$idx}"] = $this->dbEngine->quoteFieldName($column->name, 'a');
			$mapping["f{$idx}"] = $column->name;
		}

		$select = $this->dbEngine->select()
		->fields($fields)
		->from('a USE KEY (PRIMARY)', $table->name)
		->where(implode("\n\tOR\n", $conditionList));

		$select->setPreserveTypes();

		foreach($select->fetchRowsLazy() as $row) {
			$resultRow = [];
			/**
			 * @var string $alias
			 * @var int|float|string $value
			 */
			foreach($row as $alias => $value) {
				$resultRow[$mapping[$alias]] = $value;
			}
			$rowKeyHash = $table->getPrimaryKeyHash($resultRow);
			yield $rowKeyHash => $resultRow;
		}
	}

	/**
	 * @param string $tableName
	 * @param string[] $keyFields
	 * @param null|array<string, int|float|string> $lowerBound
	 * @param null|array<string, int|float|string> $upperBound
	 * @return int
	 */
	public function getRowCountBetweenLowerAndUpperBound(string $tableName, array $keyFields, ?array $lowerBound = null, ?array $upperBound = null) {
		return (int) $this->getBaseSelectWithLowerAndUpperBound($tableName, $keyFields, $lowerBound, $upperBound)
		->field('COUNT(*)')
		->fetchValue();
	}

	/**
	 * @param string $tableName
	 * @param string[] $keyFields
	 * @param null|array<string, int|float|string> $lowerBound
	 * @param null|array<string, int|float|string> $upperBound
	 * @return RunnableSelect
	 */
	private function getBaseSelectWithLowerAndUpperBound(string $tableName, array $keyFields, ?array $lowerBound = null, ?array $upperBound = null): RunnableSelect {
		$select = $this->dbEngine->select()
		->from($tableName);

		$execParams = [];
		$initLevel = 0;
		if($lowerBound !== null) {
			[$cond, $execParams, $initLevel] = $this->offsetConditionBuilder->buildGreaterThan($keyFields, $lowerBound, $execParams, $initLevel);
			$select->where($cond);
		}

		if($upperBound !== null) {
			[$cond, $execParams] = $this->offsetConditionBuilder->buildLowerOrEqualThan($keyFields, $upperBound, $execParams, $initLevel);
			$select->where($cond);
		}

		$select->bindValues($execParams);

		return $select;
	}

	/**
	 * @param iterable<int, array<string, int|float|string>> $conditionKeySets
	 * @return array<int, string>
	 */
	private function buildConditionList(iterable $conditionKeySets): array {
		$conditionList = [];
		$db = $this->dbEngine;
		foreach($conditionKeySets as $conditionKeys) {
			$condition = [];
			foreach($conditionKeys as $key => $value) {
				$condition[] = sprintf('%s=%s', $db->quoteFieldName($key, 'a'), $db->quoteValue($value));
			}
			$conditionList[] = sprintf("\t(%s)", implode(' AND ', $condition));
		}
		return $conditionList;
	}
}
