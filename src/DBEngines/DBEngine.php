<?php
namespace Kir\DBSync\DBEngines;

use Kir\DBSync\DB;
use PDO;

interface DBEngine {
	public function getPDO(): PDO;
	public function getDB(): DB;
	public function quoteFieldName(string $fieldName, ?string $alias = null): string;
	public function quoteValue(string $value): string;
}
