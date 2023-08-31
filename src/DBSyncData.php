<?php
namespace Kir\DBSync;

use Kir\DBSync\Common\AbstractStatement;
use Kir\DBSync\Common\DeleteStatement;
use Kir\DBSync\Common\InsertStatement;
use Kir\DBSync\Common\Json;
use Kir\DBSync\Common\LogEntry;
use Kir\DBSync\Common\UpdateStatement;
use Kir\DBSync\DBEngines\DBEngine;
use PDOException;
use Generator;
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
	 * @param null|callable(string, string, array<string, null|scalar>):bool $pkFilterFn
	 * @return void
	 */
	public function syncTwoTablesFromDifferentConnections(DBTable $table, DBEngine $sourceDBEngine, DBEngine $destDBEngine, $pkFilterFn = null): void {
		$changes = $this->projectChanges($table, $sourceDBEngine, $destDBEngine, $pkFilterFn);
		foreach ($changes as $change) {
			if($change instanceof AbstractStatement) {
				$destDBEngine->getPDO()->exec($change->getStatement());
			} elseif($change instanceof LogEntry) {
				$this->logger->info($change->getMessage());
			}
		}
	}

	/**
	 * @param DBTable $table
	 * @param DBEngine $sourceDBEngine
	 * @param DBEngine $destDBEngine
	 * @param null|callable(string, string, array<string, null|scalar>):bool $pkFilterFn
	 * @return Generator<AbstractStatement>
	 */
	public function getSQLChanges(DBTable $table, DBEngine $sourceDBEngine, DBEngine $destDBEngine, $pkFilterFn = null): Generator {
		$changes = $this->projectChanges($table, $sourceDBEngine, $destDBEngine, $pkFilterFn);
		foreach ($changes as $change) {
			if($change instanceof AbstractStatement) {
				yield $change;
			}
		}
	}

	/**
	 * @param DBTable $table
	 * @param DBEngine $sourceDBEngine
	 * @param DBEngine $destDBEngine
	 * @param null|callable(string, string, array<string, null|scalar>):bool $pkFilterFn
	 * @return Generator<AbstractStatement|LogEntry>
	 */
	public function projectChanges(DBTable $table, DBEngine $sourceDBEngine, DBEngine $destDBEngine, $pkFilterFn = null): Generator {
		$setup = static fn () => yield from $destDBEngine->setUp();
		$tearDown = static fn () => [];
		try {
			[$keyFields, $valueFields] = DBTools::getKeyAndValueFields($table);

			$sourceDataProvider = $sourceDBEngine->getDataProvider();
			$destDataProvider = $destDBEngine->getDataProvider();

			$offset = null;
			$limit = 1000;
			do {
				if($offset !== null) {
					yield new LogEntry(sprintf("%s / Offset: %s", $table->name, Json::encode($offset)));
				}

				$maxKey = DBTools::findNearstUpperBoundWithMaxNRows($sourceDataProvider, $destDataProvider, $table->name, $keyFields, $limit, $offset);

				if($maxKey === null) {
					break;
				}

				$localKeys = $sourceDataProvider->getKeysInLowerAndUpperBound($table->name, $keyFields, $offset, $maxKey);
				$remoteKeys = $destDataProvider->getKeysInLowerAndUpperBound($table->name, $keyFields, $offset, $maxKey);

				if($pkFilterFn !== null) {
					$localKeys = array_filter($localKeys, static fn(array $key) => $pkFilterFn('local', $table->name, $key));
					$remoteKeys = array_filter($remoteKeys, static fn(array $key) => $pkFilterFn('remote', $table->name, $key));
				}

				$sourceCompareKeys = DBTools::keysToStringKeysWithOriginalKeyAsValue($localKeys);
				$destCompareKeys = DBTools::keysToStringKeysWithOriginalKeyAsValue($remoteKeys);

				$sourceMissing = array_diff_key($destCompareKeys, $sourceCompareKeys);
				$destMissing = array_diff_key($sourceCompareKeys, $destCompareKeys);
				$equalKeys = array_values(array_intersect_key($sourceCompareKeys, $destCompareKeys));

				foreach($sourceMissing as $row) {
					yield from $setup();
					$setup = static fn() => [];
					$tearDown = static fn() => yield from $destDBEngine->tearDown();

					yield new LogEntry(sprintf("%s / Remove from dest: %s", $table->name, Json::encode($row)));
					yield new DeleteStatement($destDBEngine->makeDeleteStatement($table, $row));
				}

				/** @var Generator<array<string, null|int|float|string>> $dataRows */
				$dataRows = $sourceDataProvider->getKeyAndValueColumnsLazy($table, array_values($destMissing));
				foreach($dataRows as $dataRow) {
					try {
						yield from $setup();
						$setup = static fn() => [];
						$tearDown = static fn() => yield from $destDBEngine->tearDown();

						yield new LogEntry(sprintf("%s / Add to dest: %s", $table->name, Json::encode($table->getOnlyPrimaryKeys($dataRow))));
						yield new InsertStatement($destDBEngine->makeInsertStatement($table, $dataRow));
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

					$keys = $table->getOnlyPrimaryKeys($row);

					if(!count($updateValues)) {
						$updateValues = $table->getOnlyNonPrimaryKeys($row);
					}

					yield from $setup();
					$setup = static fn() => [];
					$tearDown = static fn() => yield from $destDBEngine->tearDown();

					yield new LogEntry(sprintf("%s / %s: %s", $table->name, $rowKeyHash, implode(', ', $differences)));
					yield new UpdateStatement($destDBEngine->makeUpdateStatement($table, $updateValues, $keys));
				}
				//endregion

				$offset = $maxKey;
			} while($offset !== null);
		} finally {
			yield from $tearDown();
		}
	}
}
