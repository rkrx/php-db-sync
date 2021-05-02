<?php
namespace Kir\DBSync;

use Kir\DBSync\DBEngines\DBEngine;

class DBOffsetConditionBuilder {
	private DBEngine $dbEngine;

	public function __construct(DBEngine $dbEngine) {
		$this->dbEngine = $dbEngine;
	}

	/**
	 * @param array<int, string> $keyFields
	 * @param array<string, int|float|string> $offsetData
	 * @param array<string, mixed> $execParams
	 * @param int $initLevel
	 * @return array{string, array<string, mixed>, int}
	 */
	public function buildGreaterThan(array $keyFields, array $offsetData, array $execParams = [], int $initLevel = 0): array {
		return $this->buildLowerBoundRecursive($keyFields, $offsetData, '', $execParams, $initLevel);
	}

	/**
	 * @param array<int, string> $keyFields
	 * @param array<string, int|float|string> $offsetData
	 * @param array<string, mixed> $execParams
	 * @param int $initLevel
	 * @return array{string, array<string, mixed>, int}
	 */
	public function buildLowerOrEqualThan(array $keyFields, array $offsetData, array $execParams = [], int $initLevel = 0): array {
		return $this->buildUpperBoundRecursive($keyFields, $offsetData, '', $execParams, $initLevel);
	}

	/**
	 * @param array<int, string> $keyFields
	 * @param array<string, int|float|string> $offsetData
	 * @param string $cond
	 * @param array<string, mixed> $execParams
	 * @param int $level
	 * @return array{string, array<string, mixed>, int}
	 */
	private function buildLowerBoundRecursive(array $keyFields, array $offsetData, string $cond, array $execParams, int $level): array {
		$keyCount = count($keyFields);
		$highestLevel = $level;
		if($keyCount > 0) {
			$paramKey = static fn($idx) => "p{$idx}";
			$keyField = array_shift($keyFields);
			$quotedFieldName = $this->dbEngine->quoteFieldName($keyField);
			$execParams[$paramKey($level)] = $offsetData[$keyField] ?? null;
			if($keyCount > 1) {
				[$innerCond, $execParams, $highestLevel] = $this->buildLowerBoundRecursive($keyFields, $offsetData, $cond, $execParams, $level + 1);
				$cond = sprintf('%1$s > :%2$s OR %1$s = :%2$s AND (%3$s)', $quotedFieldName, $paramKey($level), $innerCond);
			} else {
				$cond = sprintf('%s > :%s', $quotedFieldName, $paramKey($level));
			}
		}
		return [$cond, $execParams, ($level < 1 ? $highestLevel + 1 : $highestLevel)];
	}

	/**
	 * @param array<int, string> $keyFields
	 * @param array<string, int|float|string> $offsetData
	 * @param string $cond
	 * @param array<string, mixed> $execParams
	 * @param int $level
	 * @return array{string, array<string, mixed>, int}
	 */
	private function buildUpperBoundRecursive(array $keyFields, array $offsetData, string $cond, array $execParams, int $level): array {
		$keyCount = count($keyFields);
		if($keyCount > 0) {
			$paramKey = static fn($idx) => "p{$idx}";
			$keyField = array_shift($keyFields);
			$quotedFieldName = $this->dbEngine->quoteFieldName($keyField);
			$execParams[$paramKey($level)] = $offsetData[$keyField] ?? null;
			if($keyCount > 1) {
				[$innerCond, $execParams] = $this->buildUpperBoundRecursive($keyFields, $offsetData, $cond, $execParams, $level + 1);
				$cond = sprintf('%1$s < :%2$s OR %1$s = :%2$s AND (%3$s)', $quotedFieldName, $paramKey($level), $innerCond);
			} else {
				$cond = sprintf('%s <= :%s', $quotedFieldName, $paramKey($level));
			}
		}
		return [$cond, $execParams, $level + 1];
	}
}
