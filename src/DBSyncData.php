<?php
namespace Kir\DBSync;

use Kir\DBSync\Common\Json;
use Kir\DBSync\DBEngines\DBEngine;
use PDOException;

class DBSyncData {
	/**
	 * @param DBTable $table
	 * @param DBEngine $sourceDBEngine
	 * @param DBEngine $destDBEngine
	 */
	public static function syncTwoTablesInDifferentLocations(DBTable $table, DBEngine $sourceDBEngine, DBEngine $destDBEngine): void {
		[$keyFields, $valueFields] = DBTools::getKeyAndValueFields($table);

		$fieldSpec = DBQueryProjectionBuilder::buildQuery($keyFields, $valueFields);

		$sourceDataProvider = new DBDataProvider($sourceDBEngine);
		$destDataProvider = new DBDataProvider($destDBEngine);

		$offset = null;
		$limit = 1000;
		do {
			if($offset !== null) {
				printf("%s / Offset: %s\n", $table->name, json_encode($offset, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
			}

			$maxKey = DBTools::findNearstUpperBoundWithMaxNRows($sourceDataProvider, $destDataProvider, $table->name, $keyFields, $limit, $offset);

			if($maxKey === null) {
				break;
			}

			$localKeys = $sourceDataProvider->getKeysInLowerAndUpperBound($table->name, $keyFields, $offset, $maxKey);
			$remoteKeys = $destDataProvider->getKeysInLowerAndUpperBound($table->name, $keyFields, $offset, $maxKey);

			$sourceCompareKeys = DBTools::keysToStringKeysWithOriginalKeyAsValue($localKeys);
			$destCompareKeys = DBTools::keysToStringKeysWithOriginalKeyAsValue($remoteKeys);

			$sourceMissing = array_diff_key($destCompareKeys, $sourceCompareKeys);
			$destMissing = array_diff_key($sourceCompareKeys, $destCompareKeys);
			$equalKeys = array_values(array_intersect_key($sourceCompareKeys, $destCompareKeys));

			foreach($sourceMissing as $row) {
				printf("%s / Remove from dest: %s\n", $table->name, Json::encode($row));
				$destDBEngine->getDB()->getDB()->delete()
				->from($table->name)
				->where($row)
				->limit(1)
				->run();
			}

			$dataRows = $sourceDataProvider->getKeyAndValueColumnsLazy($table, array_values($destMissing));
			foreach($dataRows as $dataRow) {
				printf("%s / Add to dest: %s\n", $table->name, Json::encode($table->getOnlyPrimaryKeys($dataRow)));
				try {
					$destDBEngine->getDB()->getDB()->insert()
					->into($table->name)
					->addAll($dataRow, $table->primaryKeyFields)
					->addOrUpdateAll($dataRow, $table->getNonPrimaryColumnNames())
					->run();
				} catch (PDOException $e) {
					printf("%s\n", $e->getMessage());
				}
			}

			//region Detect row changes
			$aValues = $sourceDataProvider->getKeysWithHashedValues($table->name, $fieldSpec, $equalKeys);
			$bValues = $destDataProvider->getKeysWithHashedValues($table->name, $fieldSpec, $equalKeys);

			$diff = DBTools::getKeysWithDifferencesInValues($aValues, $bValues);

			$sourceRows = iterator_to_array($sourceDataProvider->getKeyAndValueColumnsLazy($table, $diff));
			$destRows = iterator_to_array($destDataProvider->getKeyAndValueColumnsLazy($table, $diff));

			$nonPrimaryKeyFields = $table->getNonPrimaryColumnNames();
			$destMySQL = $destDBEngine->getDB()->getDB();
			foreach($sourceRows as $rowKeyHash => $row) {
				$differences = [];
				$updateValues = [];
				foreach($nonPrimaryKeyFields as $valueColumnName) {
					$oldValue = $destRows[$rowKeyHash][$valueColumnName];
					$newValue = $row[$valueColumnName];
					if($oldValue !== $newValue) {
						$differences[] = sprintf('%s: %s => %s', $valueColumnName, mb_substr(Json::encode($oldValue), 0, 32), mb_substr(Json::encode($newValue), 0, 32));
						$updateValues[$valueColumnName] = $newValue;
					}
				}

				printf("%s / %s: %s\n", $table->name, $rowKeyHash, implode(', ', $differences));
				$keys = $table->getOnlyPrimaryKeys($row);

				if(!count($updateValues)) {
					$updateValues = $table->getOnlyNonPrimaryKeys($row);
				}

				$destMySQL->update()
				->table($table->name)
				->setAll($updateValues)
				->where($keys)
				->limit(1)
				->run();
			}
			//endregion

			$offset = $maxKey;
		} while($offset !== null);
	}
}
