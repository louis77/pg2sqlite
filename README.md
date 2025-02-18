# pg2sqlite

`pg2sqlite` can migrate tables from PostgreSQL to SQLite3.

## Installation

### From source

```shell
$ go install github.com/louis77/pg2sqlite
```

## Usage

```
$ pg2sqlite -h
pg2sqlite v1.1.0
Copyright © Louis Brauer <louis@brauer.family>

Options:

  -h, --help
      display help information
  --pg-url
      *Postgres connection string (i.e. postgres://localhost:5432/mydb)
  --sqlite-file
      *Path to SQLite database file (i.e. mydatabase.db)
  -t, --table
      *Name of table to export
  -s, --schema
      *Name of table schema
  --ignore-columns
      comma-separated list of columns to ignore
  --drop-table-if-exists
      DANGER: Drop target table if it already exists
  --omit-data
      Only migrate table definitions, omit data
  --confirm
      Confirm prompts with Y, useful if used in script
  --verify
      Verify that the number of rows inserted into SQLite equals the number of rows loaded from Postgres. In case of failure, exits with status code 2
  --strict
      Use STRICT table option for SQLite, see https://www.sqlite.org/stricttables.html
  --omit-pk
      Omit primary key from SQLite table
  --omit-fk
      Omit foreign keys from SQLite table 
 ```

### Example

```
$ pg2sqlite --pg-url postgres://localhost:5432/defaultdb \
            --sqlite-file mysqlite.db \
            --table daily_sales \
            --ignore-columns raw_hash

Schema of table "daily_sales"
Column                     | Type                     | PK  | Ignore
-------------------------- | ------------------------ | --- | ------
reference_id               | integer                  | Yes | No
checkin                    | date                     | Yes | No
checkout                   | date                     | Yes | No
price                      | numeric                  | No  | No
raw_hash                   | character                | No  | Yes
currency                   | character                | No  | No
ts                         | timestamp with time zone | No  | No
             
Creating Table statement:
CREATE TABLE daily_sales (         
        reference_id INTEGER, 
        checkin TEXT, 
        checkout TEXT, 
        price REAL, 
        currency TEXT,  
        ts TEXT,
        PRIMARY KEY (reference_id, checkin, checkout) 
)
Does this look ok? (Y/N) y

Estimated row count: 50042260
Loading data with this statement:
SELECT "reference_id", "checkin", "checkout", "price", "currency", "external_id", "ts" FROM results_y2021m02 T

  349783 rows/s [==>-----------------------------------------------------------------]   3%

Finished.

$ 
```

## Release history

- 1.0.0
    - Initial version     
- 1.1.0
    - Update to Go 1.20 and latest package version
    - INSERTs into SQLite are now run in a single transaction, which massively increases performance
    - Add option to create tables in SQLite with STRICT table option (--strict)
    - Postgres JSON/JSONB columns will now be converted to TEXT
    - The schema name of the PG table must be specified with --schema
    - Migrate PRIMARY KEYs by default, omit with --omit-pk
    - Migrate FOREIGN KEYs by default, omit with --omit fk

## Details

pg2sqlite works with a single connection to PostgreSQL and SQLite. To keep memory consumption low, rows are transferred without buffering.

### Workflow

`pg2sqlite` will try to complete these steps in the following order:

1. Validate the connection to Postgres
2. Validate the existence of the specified SQLite file
3. Fetch the table schema from Postgres
4. Display a `CREATE TABLE` statement for the SQLite table
5. Ask for your confirmation (can be suppressed with `--confirm`)
6. Drop target table if it already exists (if `--drop-table-if-exists` is set)
7. Estimate number of rows in the Postgres table for progress display
8. Load source table rows and inserts them while they come in
9. Verify that the number of inserted rows equals the number if loaded rows (if `--verify` is set)

#### `pg2sqlite` doesn't do:

`pg2sqlite` creates the SQLite table with its columns and primary keys (unless --omit-pk is specified).
No foreign keys, constraints or indexes are created in the SQLite table.


### TODOs

- [X] Add support for Postgres JSON/JSONB columns
- [X] Migrate Primary Keys
- [ ] Create SQLite file if it doesn't exist


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

Copyright © 2021-2023 by Louis Brauer.

Software released under GNU GPLv3 license. See [LICENSE](./LICENSE).