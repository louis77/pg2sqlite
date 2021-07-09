/*	pg2sqlite - Migrate tables from PostgresQL to SQLite
	Copyright (C) 2021  Louis Brauer

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"strings"
)

type typeMap map[string]string

var (
	typeMapings = typeMap{
		"integer":                  "INTEGER",
		"smallint":                 "INTEGER",
		"numeric":                  "REAL",
		"date":                     "TEXT",
		"array":                    "TEXT",
		"character":                "TEXT",
		"character varying":        "TEXT",
		"timestamp with time zone": "TEXT",
		"__other":                  "TEXT",
	}

	sqliteDb *sql.DB
)

func ValidateSqlite(filename, tablename string, ignoreExistingTable bool) error {
	_, err := os.Stat(filename)
	if err != nil {
		return fmt.Errorf("Unable to access sqlite3 file: %w\n", err)
	}

	connStr := fmt.Sprintf("file:%s", filename)
	sqliteDb, err = sql.Open("sqlite3", connStr)
	if err != nil {
		return fmt.Errorf("unable to open Sqlite3 database: %w", err)
	}
	if err := sqliteDb.Ping(); err != nil {
		return fmt.Errorf("unable to ping Sqlite3 database: %w", err)
	}

	if !ignoreExistingTable {
		_, err = sqliteDb.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", tablename))
		if err == nil {
			return fmt.Errorf("sqlite table %s already exists", tablename)
		}
	}

	return nil
}

func BuildCreateTableSQL(schema *TableSchema) (string, error) {
	newSchema := TableSchema{
		Name: schema.Name,
	}

	for _, col := range schema.Cols {
		if col.Ignored {
			continue
		}
		newType, err := mapColumnType(col.Type)
		if err != nil {
			return "", fmt.Errorf("error during column type mapping: %w", err)
		}
		newSchema.Cols = append(newSchema.Cols, TableColumn{Name: col.Name, Type: newType})
	}

	sqlTmpl := "CREATE TABLE " + schema.Name + " ( %s )"
	var colStrings []string
	for _, col := range newSchema.Cols {
		colStrings = append(colStrings, "\t"+col.Name+" "+col.Type)
	}
	sqlCreateString := fmt.Sprintf(sqlTmpl, strings.Join(colStrings, ", \n"))

	return sqlCreateString, nil
}

func mapColumnType(origType string) (string, error) {
	newType, ok := typeMapings[origType]
	if !ok {
		newType, ok = typeMapings["__other"]
		if !ok {
			return "", fmt.Errorf("type %s could not be mapped, this should not happen", origType)
		}
	}
	return newType, nil
}

func DropTable(tablename string) error {
	_, err := sqliteDb.Exec("DROP TABLE IF EXISTS " + tablename)
	if err != nil {
		return fmt.Errorf("unable to drop sqlite table: %w", err)
	}
	return nil
}

func CreateTable(stmt string) error {
	_, err := sqliteDb.Exec(stmt)
	if err != nil {
		return fmt.Errorf("unable to create sqlite table: %w", err)
	}

	return nil
}

func CloseSqlite() error {
	return sqliteDb.Close()
}

func InsertRow(tablename string, vals []interface{}) error {
	placeholder := strings.Join(strings.Split(strings.Repeat("?", len(vals)), ""), ", ")

	result, err := sqliteDb.Exec(fmt.Sprintf("INSERT INTO %s VALUES (%s)", tablename, placeholder), vals...)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("unable to determine affected rows: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("no row affected, this should not happend")
	}

	return nil
}
