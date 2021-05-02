<?php
namespace Kir\DBSync\Common;

use JsonException;
use RuntimeException;

class Json {
	/**
	 * @param mixed $data
	 * @return string
	 */
	public static function encode($data): string {
		try {
			return json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
		} catch(JsonException $e) {
			throw new RuntimeException($e->getMessage(), $e->getCode(), $e);
		}
	}

	/**
	 * @param string $data
	 * @return mixed
	 */
	public static function decodeAssoc(string $data) {
		try {
			return json_decode($data, true, 512, JSON_THROW_ON_ERROR);
		} catch(JsonException $e) {
			throw new RuntimeException($e->getMessage(), $e->getCode(), $e);
		}
	}
}
