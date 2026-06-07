<?php
namespace Kir\DBSync;

use Kir\DBSync\Common\Json;
use PHPUnit\Framework\TestCase;

class CompareEngineTest extends TestCase {
	public function testBasics(): void {
		$storeA = [
			[['a' => 123, 'b' => 'hello world', 'c' => 1], 'abc'],
			[['a' => 124, 'b' => 'hello world', 'c' => 2], 'abc'],
			[['a' => 126, 'b' => 'hello world', 'c' => 4], 'abc'],
		];

		$storeB = [
			[['a' => 123, 'b' => 'hello world', 'c' => 1], 'abc'],
			[['a' => 125, 'b' => 'hello world', 'c' => 3], 'abc'],
			[['a' => 126, 'b' => 'hello world', 'c' => 4], 'def'],
		];

		$buildLookup = static function ($store, $keyFields) {
			return static function (array $keys) use ($store, $keyFields) {
				$result = [];
				foreach($store as [$rowkey]) {
					foreach($keys as $key) {
						foreach($keyFields as $keyField) {
							if($rowkey[$keyField] !== $key[$keyField]) {
								continue(2);
							}
						}
						$result[] = $key;
					}
				}
				return $result;
			};
		};

		$keys = ['a', 'b', 'c'];
		$ce = new CompareEngine($keys, $buildLookup($storeA, $keys), $buildLookup($storeB, $keys));

		foreach($storeA as [$key, $hash]) {
			$ce->localStore()->add($key, $hash);
		}

		foreach($storeB as [$key, $hash]) {
			$ce->remoteStore()->add($key, $hash);
		}

		$rows = $ce->localStore()->getNewKeys();
		self::assertEquals([['a' => 124, 'b' => "hello world", 'c' => 2]], $rows);

		$rows = iterator_to_array($ce->localStore()->getNew(), false);
		self::assertEquals(['{"a":124,"b":"hello world","c":2}'], array_map('strval', $rows));

		$rows = $ce->localStore()->getChangedKeys();
		self::assertEquals([['a' => 126, 'b' => "hello world", 'c' => 4]], $rows);

		$rows = iterator_to_array($ce->localStore()->getChanged(), false);
		self::assertEquals([['a' => 126, 'b' => "hello world", 'c' => 4]], $rows);

		$rows = $ce->localStore()->getMissingKeys();
		self::assertEquals([['a' => 125, 'b' => "hello world", 'c' => 3]], $rows);

		$rows = iterator_to_array($ce->localStore()->getMissing(), false);
		self::assertEquals(['{"a":125,"b":"hello world","c":3}'], array_map('strval', $rows));

		$rows = $ce->localStore()->getUnchangedKeys();
		self::assertEquals([['a' => 123, 'b' => "hello world", 'c' => 1]], $rows);

		$rows = iterator_to_array($ce->localStore()->getUnchanged(), false);
		self::assertEquals(['{"a":123,"b":"hello world","c":1}'], array_map('strval', $rows));
	}
}
