package main

import (
	"log"
	"runtime"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/i/goku"
	//"github.com/i/goku/example/jobs"
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
			log.Printf("Worker %d failed while executing: %s-%s\n%r", worker, job.Name(), job.Version(), r)
		},
		Jobs: []goku.Job{
		//jobs.WriteMessageJob{},
		},
	}

	wp, err := goku.NewWorkerPool(config, opts)
	if err != nil {
		log.Fatalf("Error creating worker pool: %v", err)
	}

	if err := wp.Work(); err != nil {
		log.Fatalf("Error starting worker pool: %v", err)
	}
}
