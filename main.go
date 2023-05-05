/*	pg2sqlite - Migrate tables from PostgreSQL to SQLite
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
	_ "embed"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gosuri/uiprogress"
	"github.com/mkideal/cli"
)

//go:embed VERSION
var Version string

const AppName = "pg2sqlite"
const Copyright = "Copyright © Louis Brauer <louis@brauer.family>"
const BatchSize = 1000000

type stringListDecoder struct {
	List []string
}

func (d *stringListDecoder) Decode(s string) error {
	d.List = strings.Split(s, ",")
	return nil
}

type argT struct {
	cli.Helper
	// Required
	PGURL     string `cli:"*pg-url" usage:"Postgres connection string (i.e. postgres://localhost:5432/mydb)"`
	SLFile    string `cli:"*sqlite-file" usage:"Path to SQLite database file (i.e. mydatabase.db)"`
	Tablename string `cli:"*t,table" usage:"Name of table to export"`
	// Change behaviour
	IgnoreColumns     stringListDecoder `cli:"ignore-columns" usage:"comma-separated list of columns to ignore" default:""`
	DropTableIfExists bool              `cli:"drop-table-if-exists" usage:"DANGER: Drop target table if it already exists" default:"false"`
	// Comfort options
	Confirm bool `cli:"confirm" usage:"Confirm prompts with Y, useful if used in script" default:""`
	Verify  bool `cli:"verify" usage:"Verify that the number of rows inserted into SQLite equals the number of rows loaded from Postgres. In case of failure, exits with status code 2" default:"false"`
	// SQLite options
	StrictTable bool `cli:"strict" usage:"Use STRICT table option for SQLite, see https://www.sqlite.org/stricttables.html" default:"false"`
}

func (argv *argT) AutoHelp() bool {
	return argv.Help
}

func run(ctx *cli.Context) error {
	argv := ctx.Argv().(*argT)

	if err := ValidatePG(argv.PGURL); err != nil {
		log.Fatal(err)
	}

	if err := ValidateSqlite(argv.SLFile, argv.Tablename, argv.DropTableIfExists); err != nil {
		log.Fatal(err)
	}

	schema, err := FetchSchema(argv.Tablename, argv.IgnoreColumns.List)
	if err != nil {
		log.Fatal(err)
	}

	PrintSchema(schema)

	createTableSQL, err := BuildCreateTableSQL(schema, argv.StrictTable)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Will create SQLite table with the following statement:\n%s\n\n", createTableSQL)

	if !argv.Confirm && !AskYesNo("Does this look ok?") {
		log.Fatal("Cancelled")
	}

	if argv.DropTableIfExists {
		if err := DropTable(schema.Name); err != nil {
			log.Fatalf("Unable to drop target table: %v\n", err)
		}
	}

	if err := CreateTable(createTableSQL); err != nil {
		log.Fatal(err)
	}

	estimatedRows, err := EstimateRows(schema.Name)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Estimated row count: %d\n", estimatedRows)

	uiprogress.Start()
	bar := uiprogress.AddBar(int(estimatedRows))
	bar.AppendCompleted()
	bar.PrependElapsed() // TODO output avg inserts per second

	rowChan := make(chan []interface{}, BatchSize)
	finished := make(chan bool)
	transferredRows := uint64(0)

	go func() {
		tx, err := sqliteDb.Begin()
		if err != nil {
			log.Fatal("unable to begin transaction on sqlite:", err)
		}

		for row := range rowChan {
			if err := InsertRow(tx, schema.Name, row); err != nil {
				tx.Rollback()
				log.Fatalln("error inserting a row:", err)
			}
			transferredRows++
			bar.Incr()
		}
		if err := tx.Commit(); err != nil {
			log.Fatalln("error committing data to sqlite:", err)
		}
		finished <- true
	}()

	go func() {
		if err := LoadData(schema, rowChan); err != nil {
			log.Println("Error while loading data", err)
			finished <- true
		}
	}()

	<-finished
	fmt.Println("Finished.")
	fmt.Println()

	if argv.Verify {
		fmt.Println("Verifying number of rows, this could take a while...")
		rowcount, err := CountRows(schema.Name)
		if err != nil {
			log.Fatalln("Unable to verify rowcount:", err)
		}
		if rowcount != transferredRows {
			log.Println("VERIFICATION FAILED")
			log.Printf("Discrepancy: counted rows: %d, rows in SQLite table: %d", transferredRows, rowcount)
			os.Exit(2)
		}
		fmt.Println("OK, row counts match")
	}

	if err := CloseSqlite(); err != nil {
		log.Println("Unable to close Sqlite database", err)
	}

	return nil
}

func main() {
	cli.SetUsageStyle(cli.DenseManualStyle)
	fmt.Printf("%s v%s\n%s\n\n", AppName, Version, Copyright)

	os.Exit(cli.Run(new(argT), run))
}
