package main

import (
	"log"

	"github.com/i/goku"
	"github.com/i/goku/example/jobs"
)

func main() {
	j := jobs.WriteMessageJob{
		Name:    "Xzibit",
		Message: "Hey man",
	}

	// schedule the job
	if err := goku.Run(j, "lo_priority"); err != nil {
		log.Fatal(err)
	}
}
