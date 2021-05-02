<?php
namespace Kir\DBSync;

use Kir\DBSync\DBDataProvider\DBQueryFieldSpec;

class DBQueryProjectionBuilder {
	/**
	 * @param array<int, string> $keyFields
	 * @param array<int, string> $valueFields
	 * @return DBQueryFieldSpec
	 */
	public static function buildQuery(array $keyFields, array $valueFields): DBQueryFieldSpec {
		$fields = [];
		$fieldMapping = [];
		foreach(array_values($keyFields) as $idx => $keyField) {
			$fields["k$idx"] = self::escapeSqlFieldName($keyField);
			$fieldMapping["k$idx"] = $keyField;
		}
		if(count($valueFields)) {
			$fields['v'] = sprintf('SHA1(%s)', self::buildJsonObjectString($valueFields));
		}
		return new DBQueryFieldSpec(self::buildFieldList($fields), $fieldMapping);
	}

	/**
	 * @param array<int, string> $fieldNames
	 * @return string
	 */
	private static function buildJsonObjectString(array $fieldNames): string {
		$params = [];
		foreach($fieldNames as $fieldName) {
			$params[] = "'{$fieldName}'";
			$params[] = self::escapeSqlFieldName($fieldName);
		}
		return sprintf('JSON_OBJECT(%s)', implode(', ', $params));
	}

	/**
	 * @param array<string, string> $fields
	 * @return array<string, string>
	 */
	private static function buildFieldList(array $fields): array {
		$sqlFields = [];
		foreach($fields as $fieldName => $expression) {
			$sqlFields[$fieldName] = $expression;
		}
		return $sqlFields;
	}

	/**
	 * @param array<int, string> $fieldNames
	 * @param string $separator
	 * @return string
	 */
	private static function joinFieldNames(array $fieldNames, string $separator = ', '): string {
		return implode($separator, array_map(static fn($f) => self::escapeSqlFieldName($f), $fieldNames));
	}

	/**
	 * @param string $fieldName
	 * @return string
	 */
	private static function escapeSqlFieldName(string $fieldName): string {
		return "`{$fieldName}`";
	}
}
