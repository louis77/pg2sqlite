# pg2sqlite

`pg2sqlite` can migrate tables from PostgreSQL to SQLite3.

## Installation

### From source

```shell
$ go get -u git.sr.ht/~louis77/pg2sqlite
```

### Download pre-built binaries

| Platform | Intel/AMD | ARM |
| -------- | ----- | --- |
| MacOS | [Intel](https://pg2sqlite.surge.sh/pg2sqlite-darwin-amd64.gz) | [M1](https://pg2sqlite.surge.sh/pg2sqlite-darwin-arm64.gz) |
| Linux | [amd64](https://pg2sqlite.surge.sh/pg2sqlite-linux-amd64.gz) | [arm](https://pg2sqlite.surge.sh/pg2sqlite-linux-arm64.gz) |
| FreeBSD | [amd64](https://pg2sqlite.surge.sh/pg2sqlite-freebsd-amd64.gz) | [arm64](https://pg2sqlite.surge.sh/pg2sqlite-freebsd-arm.gz) |
| Windows | [amd64](https://pg2sqlite.surge.sh/pg2sqlite-windows-amd64.exe.zip) | [arm](https://pg2sqlite.surge.sh/pg2sqlite-windows-arm.exe.zip) |


## Usage

```shell
$ pg2sqlite -h

Options:

  -h, --help                   display help information
      --pg-url                *Postgres connection string (i.e. postgres://localhost:5432/mydb)
      --sqlite-file           *Path to SQLite database file (i.e. mydatabase.db)
  -t, --table                 *Name of table to export
      --confirm                Confirm prompts with Y, useful if used in script
      --ignore-columns         comma-separated list of columns to ignore
      --drop-table-if-exists   DANGER: Drop target table if it already exists
```

### Example

```shell
$ pg2sqlite --pg-url postgres://localhost:5432/defaultdb \
            --sqlite-file mysqlite.db \
            --table daily_sales \
            --ignore-columns raw_hash

Schema of table "daily_sales"
Column                     | Type                     | Ignore
-------------------------- | ------------------------ | ------
reference_id               | integer                  | No
checkin                    | date                     | No
checkout                   | date                     | No
price                      | numeric                  | No
raw_hash                   | character                | Yes
currency                   | character                | No
ts                         | timestamp with time zone | No
             
Creating Table statement:
CREATE TABLE daily_sales (         
        reference_id INTEGER, 
        checkin TEXT, 
        checkout TEXT, 
        price REAL, 
        currency TEXT,  
        ts TEXT )
Does this look ok? (Y/N) y

Estimated row count: 50042260
Loading data with this statement:
SELECT "reference_id", "checkin", "checkout", "price", "currency", "external_id", "ts" FROM results_y2021m02 T

  24s [==>-----------------------------------------------------------------]   3%

Finished.

$ 
```

## Release history

- 1.0.0
    - Initial version     

## Details

pg2sqlite works with a single connection to PostgreSQL and SQLite. To keep memory consumption low, rows are transferred
without buffering.

### Workflow

`pg2sqlite` will try to complete these steps in the following order:

1. Validate the connection to Postgres
2. Validate the existence of the specified SQLite file
3. Fetch the table schema from Postgres
4. Display a `CREATE TABLE` statement for the SQLite table
5. Ask for your confirmation (can be silenced)
6. Drop target table if it already exists
7. Estimate number of rows in the Postgres table for progress display
8. Queries source table rows and inserts them while they come in

#### `pg2sqlite` doesn't do:

`pg2sqlite` created the bare table with its columns.
No primary keys, foreign keys, constraints or indexes are created
in the SQLite table.


### TODOs

- [ ] Create SQLite file if it doesn't exist
- [ ] Silence confirmations
- [ ] Offer verification option (number of rows)


### Warnings

Be careful with the `--drop-table-if-exists` option. It *will* drop your SQLite-table without
confirmation. This is useful if you use `pg2sqlite` in a scripted context. By default, if 
the table already exists in SQLite, `pg2sqlite` will terminte the process with
an error message to avoid data loss.

### Type mapping

SQLite supports only a limited number of types aka. storage classes. Postgres' complex types like JSON, ARRAY etc. are
not supported. These fields will be converted to TEXT fields by using Postgres' ROW() constructor.

Here is a table of explicit mappings:

|PG Type | SQLite Type|
|--------|------------|
|integer, smallint | INTEGER |
|numeric|REAL|
|All other types|TEXT|

## License

Copyright © 2021 by Louis Brauer.

Software released under GNU GPLv3 license. See [LICENSE](./LICENSE).