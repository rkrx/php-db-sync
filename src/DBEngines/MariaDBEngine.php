<?php
namespace Kir\DBSync\DBEngines;

use Kir\DBSync\Common\Cache;
use Kir\DBSync\DBTable;
use Kir\DBSync\PDOWrapper;
use Kir\DBSync\DBEngines\MariaDBEngine\MariaDBDataProvider;
use Kir\DBSync\DBEngines\MariaDBEngine\MariaDBTableProvider;
use Kir\MySQL\Builder\RunnableSelect;
use Kir\MySQL\Databases\MySQL;
use PDO;
use RuntimeException;

class MariaDBEngine implements DBEngine {
	private PDOWrapper $db;
	private MySQL $mysql;
	private Cache $cache;

	public function __construct(PDOWrapper $db) {
		$this->db = $db;
		$this->mysql = new MySQL($db->getPDO());
		$this->cache = new Cache();
		if(!version_compare($this->getVersion(), '10.2.3', '>=')) {
			throw new RuntimeException('Min MariaDB-Version of 10.2.3 required');
		}
	}

	public function getPDO(): PDO {
		return $this->db->getPDO();
	}

	public function select(): RunnableSelect {
		return $this->mysql->select();
	}

	public function getTableProvider(): MariaDBTableProvider {
		return $this->cache->getOr('table-provider', fn() => new MariaDBTableProvider($this->db));
	}

	public function getDataProvider(): MariaDBDataProvider {
		return $this->cache->getOr('data-provider', fn() => new MariaDBDataProvider($this));
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
	 * @inheritDoc
	 */
	public function quoteFieldName(string $fieldName, ?string $alias = null): string {
		if($alias !== null) {
			return sprintf('`%s`.`%s`', $alias, $fieldName);
		}
		return sprintf('`%s`', $fieldName);
	}

	/**
	 * @inheritDoc
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

	/**
	 * @inheritDoc
	 */
	public function insertRow(DBTable $table, array $row) {
		$this->mysql->insert()
		->into($table->name)
		->addAll($row, $table->primaryKeyFields)
		->addOrUpdateAll($row, $table->getNonPrimaryColumnNames())
		->run();
		return true;
	}

	/**
	 * @inheritDoc
	 */
	public function updateRow(DBTable $table, array $updateValues, array $keys) {
		return $this->mysql->update()
		->table($table->name)
		->setAll($updateValues)
		->where($keys)
		->limit(1)
		->run() > 0;
	}

	/**
	 * @inheritDoc
	 */
	public function deleteRow(DBTable $table, array $row) {
		return $this->mysql->delete()
		->from($table->name)
		->where($row)
		->limit(1)
		->run() > 0;
	}
}
