<?php
namespace Kir\DBSync\Common;

class Cache {
	/** @var array<string, mixed> */
	private array $cache = [];
	
	/**
	 * @template T
	 * @param string $key
	 * @param callable(): T $fn
	 * @return T
	 */
	public function getOr(string $key, $fn) {
		if(!array_key_exists($key, $this->cache)) {
			$this->cache[$key] = $fn();
		}
		return $this->cache[$key];
	}
}