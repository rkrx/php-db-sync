<?php
namespace Kir\DBSync;

use Kir\DBSync\Common\Cache;
use Kir\MySQL\Databases\MySQL;
use PDO;
use PDOStatement;
use RuntimeException;

class PDOWrapper {
	private PDO $pdo;
	private MySQL $db;

	/**
	 * @param PDO $pdo
	 */
	public function __construct(PDO $pdo) {
		$this->pdo = $pdo;
		$this->db = new MySQL($pdo);
	}

	/**
	 * @return PDO
	 */
	public function getPDO(): PDO {
		return $this->pdo;
	}

	/**
	 * @return MySQL
	 * @deprecated
	 */
	public function getDB(): MySQL {
		return $this->db;
	}

	/**
	 * @param string $query
	 * @param array<string, mixed> $params
	 * @return string|null
	 */
	public function fetchString(string $query, array $params = []) {
		$stmt = exceptionIfNotTypeT($this->pdo->query($query), PDOStatement::class, new RuntimeException());
		$stmt->execute($params);
		return $this->useStmt($stmt, fn(PDOStatement $stmt) => scalarOrNull($stmt->fetchColumn(0)));
	}

	/**
	 * @param string $query
	 * @param array<string, mixed> $params
	 * @return array<int, mixed>
	 */
	public function fetchArray(string $query, array $params = []) {
		$stmt = exceptionIfNotTypeT($this->pdo->query($query), PDOStatement::class, new RuntimeException());
		$stmt->execute($params);
		return $this->useStmt($stmt, fn(PDOStatement $stmt) => ifFalse($stmt->fetchAll(PDO::FETCH_COLUMN), []));
	}

	/**
	 * @template T
	 * @param string $query
	 * @param array<string, mixed> $params
	 * @param callable(null|int|float|string): T $fn
	 * @return array<int, T>
	 */
	public function fetchArrayCallback(string $query, array $params, $fn) {
		$stmt = exceptionIfNotTypeT($this->pdo->prepare($query), PDOStatement::class, new RuntimeException());
		$stmt->execute($params);
		$result = $this->useStmt($stmt, fn(PDOStatement $stmt) => ifFalse($stmt->fetchAll(PDO::FETCH_COLUMN), []));
		return array_map($fn, $result);
	}

	/**
	 * @param string $query
	 * @param array<string, mixed> $params
	 * @return array<int, array<string, mixed>>
	 */
	public function fetchRows(string $query, array $params = []): array {
		$stmt = exceptionIfNotTypeT($this->pdo->prepare($query), PDOStatement::class, new RuntimeException());
		$stmt->execute($params);
		return $this->useStmt($stmt, fn(PDOStatement $stmt) => ifFalse($stmt->fetchAll(PDO::FETCH_ASSOC), []));
	}

	/**
	 * @template T
	 * @param string $query
	 * @param array<string, mixed> $params
	 * @param callable(array<string, mixed>): T $fn
	 * @return T[]
	 */
	public function fetchRowsCallback(string $query, array $params, $fn): array {
		$stmt = exceptionIfNotTypeT($this->pdo->prepare($query), PDOStatement::class, new RuntimeException());
		$stmt->execute($params);
		return array_map($fn, $this->useStmt($stmt, fn(PDOStatement $stmt) => ifFalse($stmt->fetchAll(PDO::FETCH_ASSOC), [])));
	}

	/**
	 * @param string $query
	 * @param array<string, mixed> $params
	 * @return int
	 */
	public function exec(string $query, array $params = []): int {
		$stmt = $this->pdo->prepare($query);
		$stmt->execute($params);
		return $stmt->rowCount();
	}

	/**
	 * @template T
	 * @param PDOStatement $stmt
	 * @param callable(PDOStatement): T $fn
	 * @return T
	 */
	private function useStmt(PDOStatement $stmt, $fn) {
		try {
			return $fn($stmt);
		} finally {
			$stmt->closeCursor();
		}
	}
}
