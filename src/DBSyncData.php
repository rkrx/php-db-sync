<?php
namespace Kir\DBSync;

use Kir\DBSync\Common\Json;
use Kir\DBSync\DBEngines\DBEngine;
use PDOException;
use Psr\Log\LoggerInterface;

class DBSyncData {
	private LoggerInterface $logger;

	public function __construct(LoggerInterface $logger) {
		$this->logger = $logger;
	}

	/**
	 * @param DBTable $table
	 * @param DBEngine $sourceDBEngine
	 * @param DBEngine $destDBEngine
	 */
	public function syncTwoTablesFromDifferentConnections(DBTable $table, DBEngine $sourceDBEngine, DBEngine $destDBEngine): void {
		try {
			$destDBEngine->getPDO()->exec('SET FOREIGN_KEY_CHECKS=0');

			[$keyFields, $valueFields] = DBTools::getKeyAndValueFields($table);

			$sourceDataProvider = $sourceDBEngine->getDataProvider();
			$destDataProvider = $destDBEngine->getDataProvider();

			$offset = null;
			$limit = 1000;
			do {
				if($offset !== null) {
					$this->logger->info(sprintf("%s / Offset: %s", $table->name, Json::encode($offset)));
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
					$this->logger->info(sprintf("%s / Remove from dest: %s", $table->name, Json::encode($row)));
					$destDBEngine->deleteRow($table, $row);
				}

				$dataRows = $sourceDataProvider->getKeyAndValueColumnsLazy($table, array_values($destMissing));
				foreach($dataRows as $dataRow) {
					$this->logger->info(sprintf("%s / Add to dest: %s", $table->name, Json::encode($table->getOnlyPrimaryKeys($dataRow))));
					try {
						$destDBEngine->insertRow($table, $dataRow);
					} catch (PDOException $e) {
						$this->logger->error($e->getMessage(), ['exception' => $e]);
					}
				}

				//region Detect row changes
				$aValues = $sourceDataProvider->getKeysWithHashedValues($table->name, $keyFields, $valueFields, $equalKeys);
				$bValues = $destDataProvider->getKeysWithHashedValues($table->name, $keyFields, $valueFields, $equalKeys);

				$diff = DBTools::getKeysWithDifferencesInValues($aValues, $bValues);

				$sourceRows = iterator_to_array($sourceDataProvider->getKeyAndValueColumnsLazy($table, $diff));
				$destRows = iterator_to_array($destDataProvider->getKeyAndValueColumnsLazy($table, $diff));

				$nonPrimaryKeyFields = $table->getNonPrimaryColumnNames();
				foreach($sourceRows as $rowKeyHash => $row) {
					$differences = [];
					$updateValues = [];
					foreach($nonPrimaryKeyFields as $valueColumnName) {
						$oldValue = $destRows[$rowKeyHash][$valueColumnName];
						$newValue = $row[$valueColumnName];
						if($oldValue !== $newValue) {
							$differences[] = sprintf('%s: %s => %s', $valueColumnName, str_truncate(Json::encode($oldValue), 32), str_truncate(Json::encode($newValue), 32));
							$updateValues[$valueColumnName] = $newValue;
						}
					}

					$this->logger->info(sprintf("%s / %s: %s", $table->name, $rowKeyHash, implode(', ', $differences)));
					$keys = $table->getOnlyPrimaryKeys($row);

					if(!count($updateValues)) {
						$updateValues = $table->getOnlyNonPrimaryKeys($row);
					}

					$destDBEngine->updateRow($table, $updateValues, $keys);
				}
				//endregion

				$offset = $maxKey;
			} while($offset !== null);
		} finally {
			$destDBEngine->getPDO()->exec('SET FOREIGN_KEY_CHECKS=1');
		}
	}
}
