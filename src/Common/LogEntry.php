<?php

namespace Kir\DBSync\Common;

class LogEntry {
	private string $message;

	public function __construct(string $message) {
		$this->message = $message;
	}

	public function getMessage(): string {
		return $this->message;
	}

	public function __toString(): string {
		return $this->message;
	}
}
