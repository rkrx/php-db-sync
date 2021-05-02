<?php
namespace Kir\DBSync;

use Kir\DBSync\DBEngines\DBEngine;
use PHPUnit\Framework\TestCase;

class DBOffsetConditionBuilderTest extends TestCase {
	public function testBuildLowerBound(): void {
		$mockDBEngine = $this->createMock(DBEngine::class);
		$mockDBEngine->method('quoteFieldName')->willReturnCallback(fn($f) => "[$f]");
		$offsetConditionBuilder = new DBOffsetConditionBuilder($mockDBEngine);
		[$cond, $execParams] = $offsetConditionBuilder->buildGreaterThan(['a', 'b', 'c'], ['a' => 123456, 'b' => 'de', 'c' => 2.0]);
		self::assertEquals('[a] > :p0 OR [a] = :p0 AND ([b] > :p1 OR [b] = :p1 AND ([c] > :p2))', $cond);
		self::assertEquals(['p0' => 123456, 'p1' => 'de', 'p2' => 2.0], $execParams);
	}

	public function testBuildUpperBound(): void {
		$mockDBEngine = $this->createMock(DBEngine::class);
		$mockDBEngine->method('quoteFieldName')->willReturnCallback(fn($f) => "[$f]");
		$offsetConditionBuilder = new DBOffsetConditionBuilder($mockDBEngine);
		[$cond, $execParams] = $offsetConditionBuilder->buildLowerOrEqualThan(['a', 'b', 'c'], ['a' => 123456, 'b' => 'de', 'c' => 2.0]);
		self::assertEquals('[a] < :p0 OR [a] = :p0 AND ([b] < :p1 OR [b] = :p1 AND ([c] <= :p2))', $cond);
		self::assertEquals(['p0' => 123456, 'p1' => 'de', 'p2' => 2.0], $execParams);
	}
}
