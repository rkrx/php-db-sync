<?php
namespace Kir\DBSync\DBTable;

class DBColumn {
	/** @var string COLUMN_NAME */
	public string $name;
	/** @var int ORDINAL_POSITION */
	public int $position;
	/** @var null|int|float|string COLUMN_DEFAULT */
	public $defaultValue;
	/** @var bool IS_NULLABLE */
	public bool $isNullable;
	/** @var string DATA_TYPE */
	public string $dataType;
	/** @var string CHARACTER_MAXIMUM_LENGTH */
//	public x $CHARACTER_MAXIMUM_LENGTH;
	/** @var string CHARACTER_OCTET_LENGTH */
//	public x $CHARACTER_OCTET_LENGTH;
	/** @var int NUMERIC_PRECISION */
	public ?int $numericPrecision;
	/** @var int NUMERIC_SCALE */
	public ?int $numericScale;
	/** @var string DATETIME_PRECISION */
//	public x $DATETIME_PRECISION;
	/** @var string CHARACTER_SET_NAME */
//	public x $CHARACTER_SET_NAME;
	/** @var string COLLATION_NAME */
//	public x $COLLATION_NAME;
	/** @var string COLUMN_TYPE */
//	public x $COLUMN_TYPE;
	/** @var string COLUMN_KEY */
//	public x $COLUMN_KEY;
	/** @var string EXTRA */
//	public x $EXTRA;
	/** @var string PRIVILEGES */
//	public x $PRIVILEGES;
	/** @var string|null COLUMN_COMMENT */
	public ?string $comment;
	/** @var bool IS_GENERATED */
	public bool $isGenerated = false;
	/** @var string|null GENERATION_EXPRESSION */
	public ?string $expression;

	/**
	 * @param array{name?: string, position?: int, defaultValue?: null|int|float|string, isNullable?: bool, dataType?: string, numericPrecision?: int, numericScale?: int, comment?: string|null, isGenerated?: bool, expression?: string|null} $data
	 */
	public function __construct(array $data = []) {
		foreach($data as $key => $value) {
			$this->{$key} = $value;
		}
	}
}
