package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/couchbase/gocb/v2"
)

func insert_airports(col *gocb.Collection, err error, wg *sync.WaitGroup) {
	// Open file
	f, err := os.Open("/Users/mattymaclean/apply_load/GlobalAirportDatabase.txt")
	if err != nil {
		log.Fatal(err)
	}
	// Close the file at the end of the program
	defer f.Close()

	// Read the file line by line using scanner
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		// Insert into bucket
		s := strings.Split(scanner.Text(), ":")
		upsert(col, err, s[0], s[2], s[3], s[4], fmt.Sprintf("%s, %s", s[14], s[15]), s[13])
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Decrement wait group counter
	wg.Done()
}

func read_airports(col *gocb.Collection, err error, wg *sync.WaitGroup) {
	// Open file
	f, err := os.Open("/Users/mattymaclean/apply_load/GlobalAirportDatabase.txt")
	if err != nil {
		log.Fatal(err)
	}
	// Close the file at the end of the program
	defer f.Close()

	// Read the file line by line using scanner
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		// Insert into bucket
		s := strings.Split(scanner.Text(), ":")
		fmt.Printf("Read: %v\n", read(col, err, s[0]))
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Decrement wait group counter
	wg.Done()
}

func delete_airports(col *gocb.Collection, err error, wg *sync.WaitGroup) {
	// Open file
	f, err := os.Open("/Users/mattymaclean/apply_load/GlobalAirportDatabase.txt")
	if err != nil {
		log.Fatal(err)
	}
	// Close the file at the end of the program
	defer f.Close()

	// Read the file line by line using scanner
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		// Insert into bucket
		s := strings.Split(scanner.Text(), ":")
		delete(col, err, s[0])
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Decrement wait group counter
	if wg != nil {
		wg.Done()
	}
}
