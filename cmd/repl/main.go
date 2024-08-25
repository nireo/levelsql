package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/nireo/levelsql"
)

func main() {
	filepath := flag.String("db", "", "The path to the database to interact with the repl.")
	flag.Parse()

	if filepath == nil {
		fmt.Printf("you need to provide a database file\n")
		return
	}

	dbFile := *filepath
	db, err := levelsql.OpenDB(dbFile)
	if err != nil {
		fmt.Printf("got error while opening database: %s", err)
		return
	}
	defer db.Close()

	fmt.Printf(">> ")
	bufioReader := bufio.NewScanner(os.Stdin)
	for bufioReader.Scan() {

		res, err := db.Execute(bufioReader.Text())
		if err != nil {
			fmt.Printf("error executing query: %s", err)
			return
		}

		fmt.Println(res.String())
		fmt.Printf(">> ")
	}
}
