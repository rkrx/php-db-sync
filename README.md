# db-sync

## Overview

* Synchronize data from one PDO-Connection to another.
* No changed will be made to data that is equal on both connections.
* Every table in the database must have a primary key.
* Detection of new or missing rows in the destination database is based in primary-keys.
* Detection of changed rows is made by a some Hashing-Algorithm where all values of columns outside a primary key form a hash.
* Datasets will be synchronized in chunks, so the vertical size of a table should not matter.

## Quick-Start

This project was started to have a tool to quickly pull data from specific tables into a local dev database. The goal of the project is not to have some kind of command line tool to synchronize databases. For that there are tools like [pt-table-sync](https://www.percona.com/doc/percona-toolkit/LATEST/pt-table-sync.html). Is it rather meant to address more complex scenarios that could hardly be modelled by a cli-interface.

> ### Important
> This project is not intended to sync two production servers. It is meant to sync some data from a staging or production server back to local dev machines.
>
> Even though it is not necessary, since db-sync will not write into the source database in any circumstance, it is recommended to use a db-user on the source database, that is configured to be read-only. Just to be clear.
> 
> It is intended that only data gets synchronized. The table structure should be synched in the form of migration scripts and is outside the scope of this project.

The most basic script for a source mariadb 10.2.3+ to a destination mariadb 10.2.3+ could look like this:

```php
<?php
use Kir\DBSync\DBTable;
use Kir\DBSync\PDOWrapper;
use Kir\DBSync\DBSyncData;
use Kir\DBSync\DBEngines\MariaDBEngine;
use Logger\Loggers\ResourceLogger;
use Logger\Formatters\TemplateFormatter;

require 'vendor/autoload.php';

// Bring your own PSR-Logger. This is from logger/essentials (packagist)
$logger = new TemplateFormatter(new ResourceLogger(STDOUT));

$pdoOptions = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::MYSQL_ATTR_COMPRESS => true];

$sourcePDOConn = new PDO('mysql:host=127.0.0.1;port=3312;dbname=some_db_name;charset=utf8', 'readonly1', null, $pdoOptions);
$sourceDBEngine = new MariaDBEngine(new PDOWrapper($sourcePDOConn));

$destPDOConn = new PDO('mysql:host=127.0.0.1;port=3306;dbname=some_db_name;charset=utf8', 'root', null, $pdoOptions);
$destDBEngine = new MariaDBEngine(new PDOWrapper($destPDOConn));

$tables = $sourceDBEngine->getTableProvider()->getAllTables();
$tables = array_filter($tables, fn(DBTable $table) => strpos($table->name, 'shop__stats_') !== 0);

$syncData = new DBSyncData($logger);

foreach($tables as $table) {
	$logger->info($table->name);

	try {
		$syncData->syncTwoTablesFromDifferentConnections($table, $sourceDBEngine, $destDBEngine);
	} catch (PDOException $e) {
		$logger->error($e->getMessage());
	}
}
```