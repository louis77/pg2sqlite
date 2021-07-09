# pg2sqlite

CLI tool to migrate a table from a PostgreSQL database to a SQLite3 database.

### Installation

```shell
$ go get -u github.com/louis77/pg2sqlite
```

### Usage

```shell
$ pg2sqlite -h

Options:

  -h, --help                   display help information
      --pg-url                *Postgres connection string
      --sqlite-file           *Path to SQLite database
      --table                 *Name of table to export
      --drop-table-if-exists   DANGER: Drop target table if it already exists
```

### Example

```shell
$ pg2sqlite --pg-url postgres://localhost:5432/defaultdb \
            --sqlite-file mysqlite.db \
            --table daily_sales

Schema of table "daily_sales"
Column                     | Type                    
-------------------------- | ------------------------
reference_id               | integer                 
checkin                    | date                    
checkout                   | date                    
price                      | numeric                 
currency                   | character               
ratecode                   | character varying       
ts                         | timestamp with time zone
             
Creating Table statement:
CREATE TABLE daily_sales (         
        reference_id INTEGER, 
        checkin TEXT, 
        checkout TEXT, 
        price REAL, 
        currency TEXT, 
        ratecode TEXT, 
        ts TEXT )
Does this look ok? (Y/N) y

Estimated row count: 50042260
  24s [--------------------------------------------------------------------]   0%

Finished.

$ 
```

## Details

### General

pg2sqlite works with a single connection to PostgreSQL and SQLite. To keep memory consumption low, rows are transferred
without buffering.

### Type mapping

SQLite supports only a limited number of types aka. storage classes. Postgres' complex types like JSON, ARRAY etc. are
not supported. These fields will be converted to TEXT fields by using Postgres' ROW() constructor.

Here is a table of explicit mappings:

|PG Type | SQLite Type|
|--------|------------|
|integer | INTEGER |
|smallint| INTEGER |
|numeric|REAL|
|date|TEXT|
|array|TEXT|
|character|TEXT|
|character varying|TEXT|
|timestamp with time zone|TEXT|
|All other types|TEXT|

## LICENSE

© 2021 Louis Brauer

GNU GPLv3. See [LICENSE](./LICENSE).