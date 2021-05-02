<?php
namespace Kir\DBSync\DBEngines;

use Kir\DBSync\DB;
use PDO;
use RuntimeException;

class MariaDBEngine implements DBEngine {
	private DB $db;

	public function __construct(DB $db) {
		$this->db = $db;
		if(!version_compare($this->getVersion(), '10.2.3', '>=')) {
			throw new RuntimeException('Min MariaDB-Version of 10.2.3 required');
		}
	}

	public function getPDO(): PDO {
		return $this->db->getPDO();
	}

	public function getDB(): DB {
		return $this->db;
	}

	/**
	 * @return string
	 */
	public function getVersion() {
		$versionString = (string) $this->db->fetchString('SELECT VERSION()');
		// e.g. 10.5.9-MariaDB
		if(preg_match('{^(\\d+)\\.(\\d+)(?:\\.(\\d+))?\\b.*?MariaDB}i', $versionString, $matches)) {
			[, $x, $y, $z] = $matches;
			return sprintf('%d.%d.%d', $x, $y, $z);
		}
		throw new RuntimeException("Was not able to parse version string: {$versionString}");
	}

	/**
	 * @param string $fieldName
	 * @param string|null $alias
	 * @return string
	 */
	public function quoteFieldName(string $fieldName, ?string $alias = null): string {
		if($alias !== null) {
			return sprintf('`%s`.`%s`', $alias, $fieldName);
		}
		return sprintf('`%s`', $fieldName);
	}

	/**
	 * @param null|bool|int|float|string $value
	 * @return string
	 */
	public function quoteValue($value): string {
		if(is_null($value)) {
			return 'null';
		}
		if(is_bool($value)) {
			return $value ? '1' : '0';
		}
		if(is_scalar($value) && preg_match('{^(?:\\d*\\.)?\\d+$}', (string) $value)) {
			return (string) $value;
		}
		return $this->db->getPDO()->quote((string) $value);
	}
}
