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
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func AskYesNo(prompt string) bool {
	for {
		fmt.Printf("%s (Y/N) ", prompt)
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}
		switch char {
		case 'Y', 'y':
			return true
		case 'N', 'n':
			return false
		}
	}
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

	tmpl := "%-" + strconv.Itoa(maxColLength) + "s | %-" + strconv.Itoa(maxTypeLength) + "s | %s\n"
	fmt.Printf(tmpl, "Column", "Type", "Ignore") // Header
	fmt.Printf(tmpl, strings.Repeat("-", maxColLength), strings.Repeat("-", maxTypeLength), strings.Repeat("-", 6))

	for _, col := range schema.Cols {
		fmt.Printf(tmpl, col.Name, col.Type, map[bool]string{true: "Yes", false: "No"}[col.Ignored])
	}
	fmt.Println()
}
