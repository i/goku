package main

import (
	"log"
	"time"

	"github.com/i/goku"
	"github.com/i/goku/example/jobs"
)

func main() {
	goku.Configure(
		goku.BrokerConfig{
			Hostport: "127.0.0.1:6379",
			Timeout:  time.Second,
		},
	)

	j := jobs.WriteMessageJob{
		To:      "Xzibit",
		Message: "Hey man",
	}

	// schedule the job
	if err := goku.Run(j, "lo_priority"); err != nil {
		log.Fatal(err)
	}
}
