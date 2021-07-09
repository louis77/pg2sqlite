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
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"strconv"
	"strings"
)

var (
	pgConn *pgx.Conn
)

type TableSchema struct {
	Name string
	Cols []TableColumn
}

type TableColumn struct {
	Name string
	Type string
}

func ValidatePG(connStr string) error {
	var err error
	pgConn, err = pgx.Connect(context.Background(), connStr)
	if err != nil {
		return fmt.Errorf("Unable to connect to Postgres database: %w\n", err)
	}
	return nil
}

func FetchSchema(tablename string) (*TableSchema, error) {
	rows, err := pgConn.Query(context.Background(),
		"SELECT column_name, data_type FROM information_schema.columns "+
			"WHERE table_name = $1 "+
			"ORDER BY ordinal_position", tablename)

	if err != nil {
		return nil, fmt.Errorf("Unable to fetch columns from Postgres table: %w\n", err)
	}

	defer rows.Close()

	tableSchema := TableSchema{
		Name: tablename,
	}

	colCount := 0

	for rows.Next() {
		var columnName string
		var dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return nil, fmt.Errorf("Unable to scan columns from Postgres table: %w\n", err)
		}
		tableSchema.Cols = append(tableSchema.Cols, TableColumn{
			Name: columnName,
			Type: dataType,
		})
		colCount++
	}
	if colCount == 0 {
		return nil, fmt.Errorf("table %s doesn't exist in Postgres", tablename)
	}

	return &tableSchema, nil
}

func PrintSchema(schema *TableSchema) {
	fmt.Printf("Schema of table \"%s\"\n", schema.Name)

	// Find length of widest column
	maxColLength := 0
	maxTypeLength := 0

	for _, col := range schema.Cols {
		if l := len(col.Name); maxColLength < l {
			maxColLength = l
		}
		if l := len(col.Type); maxTypeLength < l {
			maxTypeLength = l
		}
	}

	tmpl := "%-" + strconv.Itoa(maxColLength) + "s | %-" + strconv.Itoa(maxTypeLength) + "s\n"
	fmt.Printf(tmpl, "Column", "Type") // Header
	fmt.Printf(tmpl, strings.Repeat("-", maxColLength), strings.Repeat("-", maxTypeLength))

	for _, col := range schema.Cols {
		fmt.Printf(tmpl, col.Name, col.Type)
	}

}

func LoadData(tablename string, out chan []interface{}) error {
	sqlTmpl := "SELECT * FROM %s T"
	sqlStmt := fmt.Sprintf(sqlTmpl, tablename)

	rows, err := pgConn.Query(context.Background(), sqlStmt)
	if err != nil {
		return fmt.Errorf("unable to load data: %w", err)
	}
	defer rows.Close()

	rowCounter := 0
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return fmt.Errorf("unable to get row values: %w", err)
		}
		out <- vals

		rowCounter++

	}
	close(out)

	if rowCounter == 0 {
		return fmt.Errorf("no rows in source table found")
	}
	return nil
}

func EstimateRows(tablename string) (uint64, error) {
	rows, err := pgConn.Query(context.Background(), "SELECT reltuples AS estimate FROM pg_class where relname = $1 LIMIT 1", tablename)
	if err != nil {
		return 0, fmt.Errorf("unable to estimate rows in postgres table: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("unable to estimate rows, no estimate returned")
	}

	var rowcount uint64
	if err := rows.Scan(&rowcount); err != nil {
		return 0, fmt.Errorf("unable to estimate rows: %w", err)
	}

	return rowcount, nil
}
