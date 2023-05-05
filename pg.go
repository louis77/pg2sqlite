/*	pg2sqlite - Migrate tables from PostgresQL to SQLite
	Copyright (C) Louis Brauer

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
	"strings"

	"github.com/jackc/pgx/v5"
)

var (
	pgConn *pgx.Conn
)

type TableSchema struct {
	Name string
	Cols []TableColumn
}

type TableColumn struct {
	Name    string
	Type    string
	Ignored bool
}

func ValidatePG(connStr string) error {
	var err error
	pgConn, err = pgx.Connect(context.Background(), connStr)
	if err != nil {
		return fmt.Errorf("unable to connect to Postgres database: %w", err)
	}
	return nil
}

func FetchSchema(tablename string, ignoredColumns []string) (*TableSchema, error) {
	rows, err := pgConn.Query(context.Background(),
		"SELECT column_name, data_type FROM information_schema.columns "+
			"WHERE table_name = $1 "+
			"ORDER BY ordinal_position", tablename)

	if err != nil {
		return nil, fmt.Errorf("unable to fetch columns from Postgres table: %w", err)
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
			return nil, fmt.Errorf("unable to scan columns from Postgres table: %w", err)
		}
		tableSchema.Cols = append(tableSchema.Cols, TableColumn{
			Name:    columnName,
			Type:    dataType,
			Ignored: Contains(ignoredColumns, columnName),
		})
		colCount++
	}
	if colCount == 0 {
		return nil, fmt.Errorf("table %s doesn't exist in Postgres", tablename)
	}

	return &tableSchema, nil
}

func LoadData(schema *TableSchema, out chan []interface{}) error {
	var colListArray []string
	for _, col := range schema.Cols {
		if col.Ignored {
			continue
		}
		colListArray = append(colListArray, fmt.Sprintf(`"%s"`, col.Name))
	}

	sqlStmt := fmt.Sprintf("SELECT %s FROM %s T", strings.Join(colListArray, ", "), schema.Name)
	fmt.Println("Loading data with this statement:")
	fmt.Println(sqlStmt)
	fmt.Println()

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
