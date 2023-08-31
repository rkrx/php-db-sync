<?php
namespace Kir\DBSync;

use Kir\DBSync\Common\Json;
use RuntimeException;

class DBTools {
	/**
	 * @param DBTable $table
	 * @return array{array<int, string>, array<int, string>} An array with idx0 = array of key-fields, idx 1 = array of value fields ()
	 */
	public static function getKeyAndValueFields(DBTable $table): array {
		$keyFields = $table->primaryKeyFields;
		$valueFields = [];
		foreach($table->columns as $column) {
			if(!in_array($column->name, $keyFields, true)) {
				$valueFields[] = $column->name;
			}
		}
		return [$keyFields, $valueFields];
	}

	/**
	 * @param array<int, array<string, int|float|string>> $array
	 * @return array<string, array<string, int|float|string>>
	 */
	public static function keysToStringKeysWithOriginalKeyAsValue(array $array): array {
		$result = [];
		foreach($array as $oldKey) {
			$newKey = [];
			foreach($oldKey as $key => $value) {
				$newKey[$key] = (string) $value;
			}
			$key = Json::encode($newKey);
			$result[$key] = $oldKey;
		}
		return $result;
	}

	/**
	 * @param DBDataProvider $firstProvider
	 * @param DBDataProvider $secondProvider
	 * @param string $tableName
	 * @param string[] $keyFields
	 * @param int $limit
	 * @param null|array<string, int|float|string> $offset
	 * @return null|array<string, int|float|string>
	 */
	public static function findNearstUpperBoundWithMaxNRows(DBDataProvider $firstProvider, DBDataProvider $secondProvider, string $tableName, array $keyFields, int $limit, ?array $offset): ?array {
		$maxKey = $firstProvider->getGreatestKeyInRange($tableName, $keyFields, $limit, $offset);
		$destCount = $secondProvider->getRowCountBetweenLowerAndUpperBound($tableName, $keyFields, $offset, $maxKey);
		$sourceCount = $firstProvider->getRowCountBetweenLowerAndUpperBound($tableName, $keyFields, $offset, $maxKey);

		if($sourceCount === 0 && $destCount === 0) {
			return null;
		}

		if($destCount > $limit) {
			// The object-count within the range in greater on the remote side
			return $secondProvider->getGreatestKeyInRange($tableName, $keyFields, $limit, $offset);
		}

		return $maxKey;
	}

	/**
	 * @param array<string, array{hash: string, keys: array<string, int|float|string>}> $aKV
	 * @param array<string, array{hash: string, keys: array<string, int|float|string>}> $bKV
	 * @return array<int, array<string, int|float|string>>
	 */
	public static function getKeysWithDifferencesInValues(array $aKV, array $bKV): array {
		$intersectingKeys = array_values(array_intersect(array_keys($aKV), array_keys($bKV)));
		$result = [];
		foreach($intersectingKeys as $key) {
			if($aKV[$key]['hash'] !== $bKV[$key]['hash']) {
				$result[] = $aKV[$key]['keys'];
			}
		}
		return $result;
	}
}
