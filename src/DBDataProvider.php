<?php
namespace Kir\DBSync;

use Kir\DBSync\DBDataProvider\DBQueryFieldSpec;
use Kir\DBSync\DBEngines\DBEngine;
use Kir\MySQL\Builder\RunnableSelect;
use Kir\MySQL\Databases\MySQL;
use Generator;

class DBDataProvider {
	private MySQL $mysql;
	private DBOffsetConditionBuilder $offsetConditionBuilder;
	private DBEngine $dbEngine;

	public function __construct(DBEngine $dbEngine) {
		$this->dbEngine = $dbEngine;
		$this->mysql = new MySQL($dbEngine->getPDO());
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
		$transferFields = [];
		foreach($keyFields as $key => $fieldName) {
			$fields["k{$key}"] = sprintf('`a`.`%s`', $fieldName);
			$resultFields["k{$key}"] = $fieldName;
			$transferFields["k{$key}"] = "t.k{$key}";
		}

		$select = $this->mysql->select()
		->fields($fields)
		->from('a', $tableName)
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
			foreach($rowLastRow as $key => $value) {
				$result[$resultFields[$key]] = $value;
			}
			return $result;
		}

		return null;
	}

	/**
	 * @param string $tableName
	 * @param DBQueryFieldSpec $fieldSpec
	 * @param iterable<int, array<string, mixed>> $equalKeySets
	 * @return array<string, array{hash: string, keys: array<string, int|float|string>}>
	 */
	public function getKeysWithHashedValues(string $tableName, DBQueryFieldSpec $fieldSpec, iterable $equalKeySets): array {
		$conditionList = $this->buildConditionList($equalKeySets);

		if(!count($conditionList)) {
			return [];
		}

		$select = $this->mysql->select()
		->fields($fieldSpec->getDBFields())
		->from('a', $tableName)
		->where(implode("\n\tOR\n", $conditionList));

		$select->setPreserveTypes();
//		$select->debug();

		return $fieldSpec->translateFields($select->fetchRowsLazy());
	}

	/**
	 * @param DBTable $table
	 * @param iterable<int, array<string, mixed>> $keySets
	 * @return Generator<string, array<string, mixed>>
	 */
	public function getKeyAndValueColumnsLazy(DBTable $table, iterable $keySets): Generator {
		$conditionList = $this->buildConditionList($keySets);

		if(!count($conditionList)) {
			return [];
		}

		$mapping = [];
		$fields = [];
		foreach(array_values($table->columns) as $idx => $column) {
			if($column->isGenerated) {
				continue;
			}
			$fields["f{$idx}"] = $this->dbEngine->quoteFieldName($column->name, 'a');
			$mapping["f{$idx}"] = $column->name;
		}

		$select = $this->mysql->select()
		->fields($fields)
		->from('a', $table->name)
		->where(implode("\n\tOR\n", $conditionList));

		$select->setPreserveTypes();

		foreach($select->fetchRowsLazy() as $row) {
			$resultRow = [];
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
		$select = $this->mysql->select()
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
	 * @param iterable<int, array<string, mixed>> $conditionKeySets
	 * @return array<int, string>
	 */
	private function buildConditionList(iterable $conditionKeySets): array {
		$conditionList = [];
		foreach($conditionKeySets as $conditionKeys) {
			$condition = [];
			foreach($conditionKeys as $key => $value) {
				$condition[] = sprintf('%s.%s=%s', $this->mysql->quoteField('a'), $this->mysql->quoteField($key), $this->mysql->quote($value));
			}
			$conditionList[] = sprintf("\t(%s)", implode(' AND ', $condition));
		}
		return $conditionList;
	}
}
