<?php

namespace Kir\DBSync\Common;

abstract class AbstractStatement {
	private string $statement;

	public function __construct(string $statement) {
		$this->statement = $statement;
	}

	public function getStatement(): string {
		return $this->statement;
	}

	public function __toString(): string {
		return $this->statement;
	}
}
