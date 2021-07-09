/*	pg2sqlite - Migrate tables from PostgreSQL to SQLite
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
	"fmt"
	"github.com/gosuri/uiprogress"
	"github.com/mkideal/cli"
	"log"
	"os"
	"strings"
)

type stringListDecoder struct {
	List []string
}

func (d *stringListDecoder) Decode(s string) error {
	d.List = strings.Split(s, ",")
	return nil
}

type argT struct {
	cli.Helper
	PGURL             string            `cli:"*pg-url" usage:"Postgres connection string (i.e. postgres://localhost:5432/mydb)"`
	SLFile            string            `cli:"*sqlite-file" usage:"Path to SQLite database file (i.e. mydatabase.db)"`
	Tablename         string            `cli:"*t,table" usage:"Name of table to export"`
	Confirm           bool              `cli:"confirm" usage:"Confirm prompts with Y, useful if used in script" default:""`
	IgnoreColumns     stringListDecoder `cli:"ignore-columns" usage:"comma-separated list of columns to ignore" default:""`
	DropTableIfExists bool              `cli:"drop-table-if-exists" usage:"DANGER: Drop target table if it already exists" default:"false"`
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

	createTableSQL, err := BuildCreateTableSQL(schema)
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

	rowcount, err := EstimateRows(schema.Name)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Estimated row count: %d\n", rowcount)

	uiprogress.Start()
	bar := uiprogress.AddBar(int(rowcount))
	bar.AppendCompleted()
	bar.PrependElapsed()

	rowChan := make(chan []interface{})
	finished := make(chan bool)

	go func() {
		for row := range rowChan {
			if err := InsertRow(schema.Name, row); err != nil {
				log.Fatalln("error inserting a row:", err)
			}
			bar.Incr()
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

	if err := CloseSqlite(); err != nil {
		log.Println("Unable to close Sqlite database", err)
	}

	return nil
}

func main() {
	os.Exit(cli.Run(new(argT), run))
}
