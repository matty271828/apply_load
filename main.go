package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

func main() {
	// Connect to cluster
	connectionString := "localhost:12000"
	bucketName := "airports"
	username := "admin"
	password := "password"

	// For a secure cluster connection, use `couchbases://<your-cluster-ip>` instead.
	cluster, err := gocb.Connect("couchbase://"+connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(30*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Connected to bucket: %s\n", bucketName)
	}

	// Get a reference to the default collection
	col := bucket.Scope("_default").Collection("_default")

	// Initialise wait group
	wg := sync.WaitGroup{}

	// Start concurrent threads inserting, deleting and reading the same data to/from the cluster
	// N.B this is an infinite loop and will need to be terminated via ctrl-c
	for {
		// TODO:
		wg.Add(1)
		go insert_airports(col, err, &wg)

		wg.Add(1)
		go read_airports(col, err, &wg)

		wg.Add(1)
		go delete_airports(col, err, &wg)

		// Ensure threads finished executing before exiting
		wg.Wait()
	}

}
