<?php
namespace Kir\DBSync;

use Throwable;

/**
 * @template T
 * @template U
 * @param T|false $value
 * @param U $valueIfFalse
 * @return T|U
 */
function ifFalse($value, $valueIfFalse) {
	if($value === false) {
		return $valueIfFalse;
	}
	return $value;
}

/**
 * @template T
 * @param T|false $value
 * @return T|null
 */
function nullIfFalse($value) {
	return $value === false ? null : $value;
}

/**
 * @template T
 * @template U of Throwable
 * @param mixed $value
 * @param class-string<T> $t
 * @param U $e
 * @throws U
 * @return T
 */
function exceptionIfNotTypeT($value, $t, Throwable $e) {
	if(gettype($value) === $t || is_subclass_of($value, $t) || is_a($value, $t)) {
		return $value;
	}
	throw $e;
}

/**
 * @param mixed $value
 * @return string|null
 */
function scalarOrNull($value) {
	if($value === null) {
		return null;
	}
	if(is_scalar($value)) {
		return (string) $value;
	}
	return null;
}
