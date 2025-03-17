<?php
namespace Kir\DBSync\DBEngines;

use Kir\DBSync\Common\Cache;
use Kir\DBSync\Common\ExecuteStatement;
use Kir\DBSync\DBTable;
use Kir\DBSync\PDOWrapper;
use Kir\DBSync\DBEngines\MySQLEngine\MySQLDataProvider;
use Kir\DBSync\DBEngines\MySQLEngine\MySQLTableProvider;
use Kir\MySQL\Builder\RunnableSelect;
use Kir\MySQL\Databases\MySQL;
use PDO;
use Generator;
use RuntimeException;

class MySQLEngine implements DBEngine {
	private PDOWrapper $db;
	private MySQL $mysql;
	private Cache $cache;

	/**
	 * @param PDOWrapper $db
	 */
	public function __construct(PDOWrapper $db) {
		$this->db = $db;
		$this->mysql = new MySQL($db->getPDO());
		$this->cache = new Cache();
		if(!version_compare($this->getVersion(), '8.0', '>=')) {
			throw new RuntimeException('Min MySQL-Version of 8.0 required');
		}
	}

	/**
	 * @return PDO
	 */
	public function getPDO(): PDO {
		return $this->db->getPDO();
	}

	/**
	 * @return RunnableSelect
	 */
	public function select(): RunnableSelect {
		return $this->mysql->select();
	}

	/**
	 * @return MySQLTableProvider
	 */
	public function getTableProvider(): MySQLTableProvider {
		return $this->cache->getOr('table-provider', fn() => new MySQLTableProvider($this->db));
	}

	/**
	 * @return MySQLDataProvider
	 */
	public function getDataProvider(): MySQLDataProvider {
		return $this->cache->getOr('data-provider', fn() => new MySQLDataProvider($this));
	}

	/**
	 * @return string
	 */
	public function getVersion() {
		$versionString = (string) $this->db->fetchString('SELECT VERSION()');
		// e.g. 8.0.36-28
		if(preg_match('{^(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:-\\d+)?\\b}i', $versionString, $matches)) {
			[, $x, $y] = $matches;
			$z = $matches[3] ?? '';
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
		// @phpstan-ignore-next-line
		if(is_scalar($value) && preg_match('{^(?:\\d*\\.)?\\d+$}', (string) $value)) {
			return (string) $value;
		}
		return $this->db->getPDO()->quote((string) $value);
	}

	public function setUp(): Generator {
		yield new ExecuteStatement('SET FOREIGN_KEY_CHECKS=0;');
	}

	public function tearDown(): Generator {
		yield new ExecuteStatement('SET FOREIGN_KEY_CHECKS=1;');
	}

	/**
	 * @inheritDoc
	 */
	public function makeInsertStatement(DBTable $table, array $row) {
		return $this->postProcessStatement((string) $this->mysql->insert()
		->into($table->name)
		->addAll($row, $table->primaryKeyFields)
		->addOrUpdateAll($row, $table->getNonPrimaryColumnNames()));
	}

	/**
	 * @inheritDoc
	 */
	public function makeUpdateStatement(DBTable $table, array $updateValues, array $keys) {
		return $this->postProcessStatement((string) $this->mysql->update()
		->table($table->name)
		->setAll($updateValues)
		->where($keys)
		->limit(1));
	}

	/**
	 * @inheritDoc
	 */
	public function makeDeleteStatement(DBTable $table, array $row) {
		return $this->postProcessStatement((string) $this->mysql->delete()
		->from($table->name)
		->where($row)
		->limit(1));
	}

	private function postProcessStatement(string $statement): string {
		$parts = [['sql', '']];
		$idx = 0;
		$exitChar = null;
		for($i = 0, $l = strlen($statement); $i < $l; $i++) {
			if($parts[$idx][0] === 'sql' && in_array($statement[$i], ['"', '\'', '`'])) {
				$parts[] = ['string', ''];
				$idx = count($parts) - 1;
				$parts[$idx][1] .= $statement[$i];
				$exitChar = $statement[$i];
				continue;
			}

			if($parts[$idx][0] === 'string' && $statement[$i] === '\\') {
				$parts[$idx][1] .= $statement[$i];
				$i++;
				$parts[$idx][1] .= $statement[$i];
				continue;
			}

			if($parts[$idx][0] === 'string' && $statement[$i] === $exitChar) {
				$parts[$idx][1] .= $statement[$i];
				$parts[] = ['sql', ''];
				$idx = count($parts) - 1;
				$exitChar = null;
				continue;
			}

			$parts[$idx][1] .= $statement[$i];
		}

		$statementParts = [];
		foreach ($parts as $part) {
			if($part[0] === 'sql') {
				$statementParts[] = (string) preg_replace('{\\s+}',   ' ',   $part[1]);
			} else {
				$statementParts[] = $part[1];
			}
		}
		$statement = implode('', $statementParts);

		return sprintf("%s;", rtrim($statement, " \r\n\t;"));
	}
}
