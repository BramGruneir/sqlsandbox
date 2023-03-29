package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	pgx "github.com/jackc/pgx/v4"
)

// Local
var config = "postgresql://root@127.0.0.1:26257/defaultdb?options=-ccluster%3Ddemo-tenant&sslmode=disable"

var testSize = 100000
var runCount = 100000

func main() {
	ctx := context.Background()

	// create the connection
	connectionConfig, err := pgx.ParseConfig(config)
	if err != nil {
		panic(err)
	}

	conn, err := pgx.ConnectConfig(ctx, connectionConfig)
	if err != nil {
		panic(err)
	}

	// Create the database sandbox
	if _, err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS sandbox"); err != nil {
		panic(err)
	}
	fmt.Print("created sandbox\n")

	// Create the table test
	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS sandbox.test"); err != nil {
		panic(err)
	}
	var createTestTable = `
			  CREATE TABLE sandbox.test (
			  	a INT PRIMARY KEY,
			  	b INT
			  )`
	if _, err := conn.Exec(ctx, createTestTable); err != nil {
		panic(err)
	}
	fmt.Print("created sandbox.test\n")

	// Add all the rows
	for i := 0; i < testSize; i++ {
		if _, err := conn.Exec(ctx, "UPSERT INTO sandbox.test VALUES ($1,$2)", i, i); err != nil {
			panic(err)
		}
		if (i+1)%10000 == 0 {
			fmt.Printf("Added %v rows\n", i+1)
		}
	}

	time.Sleep(time.Second * 10)

	// try the implicit reads
	fmt.Print("Starting implicit Test\n")
	start := time.Now()
	for i := 0; i < runCount; i++ {
		r := rand.Intn(testSize)
		row := conn.QueryRow(ctx, "SELECT b FROM sandbox.test AS OF SYSTEM TIME '-10s' WHERE a = $1", r)
		var receiver int
		if err := row.Scan(&receiver); err != nil {
			panic(err)
		}
		if (i+1)%(runCount/10) == 0 {
			fmt.Printf("SELECTED %v rows\n", i+1)
		}
	}
	end := time.Now()
	nanos := (end.UnixNano() - start.UnixNano()) / int64(runCount)
	fmt.Printf("Took %v sec - or %d ns per request\n", end.Sub(start), nanos)

	// try the explicit reads
	fmt.Print("Starting explicit Test\n")
	start = time.Now()
	for i := 0; i < runCount; i++ {
		r := rand.Intn(testSize)
		tx, err := conn.Begin(ctx)
		if err != nil {
			panic(err)
		}
		tx.Exec(ctx, "SET TRANSACTION AS OF SYSTEM TIME '-10s'")
		row := tx.QueryRow(ctx, "SELECT b FROM sandbox.test WHERE a = $1", r)
		var receiver int
		if err := row.Scan(&receiver); err != nil {
			panic(err)
		}
		if err := tx.Commit(ctx); err != nil {
			panic(err)
		}

		if (i+1)%(runCount/10) == 0 {
			fmt.Printf("SELECTED %v rows\n", i+1)
		}
	}
	end = time.Now()
	nanos = (end.UnixNano() - start.UnixNano()) / int64(runCount)
	fmt.Printf("Took %v sec - or %d ns per request\n", end.Sub(start), nanos)
}
