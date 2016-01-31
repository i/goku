package main

import (
	"log"
	"runtime"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/i/goku"
	"github.com/i/goku/example/jobs"
)

var rc redis.Conn

func init() {
	goku.Register(
		jobs.WriteMessageJob{},
	)
}

func main() {
	numWorkers := runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	config := goku.WorkerConfig{
		NumWorkers:   numWorkers,
		PollInterval: time.Second,
		Queue:        "lo_priority",
		Failure: func(worker int, jobName string, r interface{}) {
			log.Printf("Worker %d failed while executing: %s\n%r", worker, jobName, r)
		},
	}

	jobs := []goku.Job{
		jobs.WriteMessageJob{},
	}

	if err := goku.Work(config, jobs); err != nil {
		log.Fatal(err)
	}
}
