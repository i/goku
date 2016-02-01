package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/i/goku"
	"github.com/i/goku/example/jobs"
)

var rc redis.Conn

func main() {
	numWorkers := runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	config := goku.WorkerConfig{
		NumWorkers: numWorkers,
		Queues:     []string{"lo_priority"},
		Hostport:   "127.0.0.1:6379",
		Timeout:    time.Second,
	}

	opts := goku.WorkerPoolOptions{
		Failure: func(worker int, job goku.Job, r interface{}) {
			log.Printf("Worker %d failed while executing: %s\n%v\n", worker, job.Name(), r)
		},
		Jobs: []goku.Job{
			jobs.WriteMessageJob{},
		},
	}

	wp, err := goku.NewWorkerPool(config, opts)
	if err != nil {
		log.Fatalf("Error creating worker pool: %v", err)
	}

	wp.Start()
	fmt.Printf("Started %d workers\n", config.NumWorkers)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c

	fmt.Println("Shutting down...")
	wp.Stop()
	os.Exit(0)
}
