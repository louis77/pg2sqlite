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
	"bufio"
	"fmt"
	"os"
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
