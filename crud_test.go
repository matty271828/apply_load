package main

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/go-cmp/cmp"
)

// Package-level varaiables
var col *gocb.Collection
var err error

func TestMain(m *testing.M) {
	// Parameters
	connectionString := "localhost:12000"
	bucketName := "test"
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
	col = bucket.Scope("_default").Collection("_default")

	// Insert single document into bucket
	upsert(col, err, "LPL", "Liverpool John Lennon Airport", "Liverpool", "United Kingdom", "53.334,-2.850", "00025")

	// call the Run method on *testing.M to run the test functions
	exitVal := m.Run()
	os.Exit(exitVal)

	// TODO: ensure test bucket once again empty - remove all documents
	delete_airports(col, err, nil)
}

// Test read operation
func Test_read(t *testing.T) {
	// Define struct expected to return
	expected := Airport{
		Name:     "Liverpool John Lennon Airport",
		City:     "Liverpool",
		Country:  "United Kingdom",
		Coords:   "53.334,-2.850",
		Altitude: "00025",
	}

	// Check for key already present in bucket
	result := read(col, err, "LPL")

	// Compare expected result of the function to the returned result
	if diff := cmp.Diff(expected, result); diff != "" {
		// Fatal called as Test_delete relies on prescence of "LPL"
		t.Fatal(diff)
	}
}

// Test delete operation
func Test_delete(t *testing.T) {
	// Delete key "LPL"
	delete(col, err, "LPL")

	// Call read - nothing should return
	expected := Airport{}
	result := read(col, err, "LPL")

	if diff := cmp.Diff(expected, result); diff != "" {
		// Fatal called as Test_upsert must start with "LPL" not present
		t.Fatal(diff)
	}
}

// Test create operation
func Test_upsert(t *testing.T) {
	// Insert into to database with key not yet present
	upsert(col, err, "LPL", "Liverpool John Lennon Airport", "Liverpool", "United Kingdom", "53.334,-2.850", "00025")

	// Read and check if present
	// Define struct expected to return
	expected := Airport{
		Name:     "Liverpool John Lennon Airport",
		City:     "Liverpool",
		Country:  "United Kingdom",
		Coords:   "53.334,-2.850",
		Altitude: "00025",
	}

	// Check for key already present in bucket
	// Compare expected result of the function to the returned result
	result := read(col, err, "LPL")
	if diff := cmp.Diff(expected, result); diff != "" {
		// Fatal called as Test_delete relies on prescence of "LPL"
		t.Fatal(diff)
	}

	// Upsert into database for key already present
	upsert(col, err, "LPL", "Liverpool John Lennon Airport", "Liverpool", "United Kingdom", "53.334,-2.850", "00025")

	// Read and check if present
	result = read(col, err, "LPL")
	if diff := cmp.Diff(expected, result); diff != "" {
		// Fatal called as Test_delete relies on prescence of "LPL"
		t.Fatal(diff)
	}
}
