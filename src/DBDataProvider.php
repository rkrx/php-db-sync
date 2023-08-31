<?php

namespace Kir\DBSync;

use Generator;

interface DBDataProvider {
	/**
	 * @param string $tableName
	 * @param string[] $keyFields
	 * @param null|array<string, int|float|string> $lowerBound
	 * @param null|array<string, int|float|string> $upperBound
	 * @return array<int, array<string, int|float|string>>
	 */
	public function getKeysInLowerAndUpperBound(string $tableName, array $keyFields, ?array $lowerBound, ?array $upperBound): array;

	/**
	 * @param string $tableName
	 * @param array<int, string> $keyFields
	 * @param int $limit
	 * @param null|array<string, int|float|string> $offset
	 * @return null|array<string, int|float|string>
	 */
	public function getGreatestKeyInRange(string $tableName, array $keyFields, int $limit, ?array $offset = null): ?array;

	/**
	 * @param string $tableName
	 * @param string[] $keyFields
	 * @param string[] $valueFields
	 * @param iterable<int, array<string, mixed>> $equalKeySets
	 * @return array<string, array{hash: string, keys: array<string, int|float|string>}>
	 */
	public function getKeysWithHashedValues(string $tableName, array $keyFields, array $valueFields, iterable $equalKeySets): array;

	/**
	 * @param DBTable $table
	 * @param iterable<int, array<string, mixed>> $keySets
	 * @return Generator<string, array<string, mixed>>
	 */
	public function getKeyAndValueColumnsLazy(DBTable $table, iterable $keySets): Generator;

	/**
	 * @param string $tableName
	 * @param string[] $keyFields
	 * @param null|array<string, int|float|string> $lowerBound
	 * @param null|array<string, int|float|string> $upperBound
	 * @return int
	 */
	public function getRowCountBetweenLowerAndUpperBound(string $tableName, array $keyFields, ?array $lowerBound = null, ?array $upperBound = null);
}
