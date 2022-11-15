package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
)

// Struct definition
type Airport struct {
	Name     string `json:"name"`
	City     string `json:"city"`
	Country  string `json:"country"`
	Coords   string `json:"coords"`
	Altitude string `json:"altitude"`
}

// CREATE operation
func upsert(col *gocb.Collection, err error, key string, name string,
	city, country string, coords string, altitude string) {
	_, err = col.Upsert(key,
		Airport{
			Name:     name,
			City:     city,
			Country:  country,
			Coords:   coords,
			Altitude: altitude,
		}, nil)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Inserted: %s: {%s, %s, %s, %s, %s}\n", key, name, city, country, coords, altitude)
	}
}

// READ operation
func read(col *gocb.Collection, err error, key string) Airport {
	getResult, err := col.Get(key, nil)
	var inAirport Airport

	if err != nil {
		if strings.Split(err.Error(), "|")[0] == "document not found " {
			// do nothing
		} else {
			panic(err)
		}
		return inAirport
	} else {
		// Key found in database, read value
		err = getResult.Content(&inAirport)

		if err != nil {
			panic(err)
		}

		return inAirport
	}
}

// DELETE operation
func delete(col *gocb.Collection, err error, key string) {
	// Remove with Durability
	_, err = col.Remove(key, &gocb.RemoveOptions{
		Timeout:         100 * time.Millisecond,
		DurabilityLevel: gocb.DurabilityLevelMajority,
	})
	if err != nil {
		error_type := strings.Split(err.Error(), "|")[0]
		if error_type == "document not found " || error_type == "durability ambiguous " {
			// do nothing
		} else {
			panic(err)
		}
	} else {
		fmt.Printf("Deleted: %s\n", key)
	}
}
