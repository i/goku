goku
========
distributed task queue for go

how to get
------------

    go get github.com/i/goku


defining jobs
--------

```go
package jobs

import "email"

/*
  Defining jobs is easy. A job only needs to implement two methods: Name() and Execute().
  Name() simply returns the name of the job. The name should be unique so that workers know how what type of job it is and if they can correctly process it.
*/

// All fields used in Execute() must be exported!
type SendEmailJob struct {
  To         string
  From       string
  Subject    string
  Body       string
}

// Receivers of Execute() must be structs.
func (j SendEmailJob) Execute() error {
  return email.Send(j.To, j.From, j.Subject, j.Body)
}

// The return value from Name() should be unique from other jobs because it 
// is used to differentiate between different jobs.
func (j SendEmailJob) Name() error {
  return "send_email_job_v0"
}

```

how to queue up jobs
---------

```go
package main

import (
  "time"

  "github.com/i/goku"

  "./jobs"
)

func main() {
  err := goku.Configure(goku.BrokerConfig{
    Hostport: "127.0.0.1:6379",
    Timeout:  time.Second
  })
  if err != nil {
    log.Fatalf("Couldn't configure goku: %v", err)
  }

  job := jobs.SendEmailJob{
    To:      "Will Smith",
    From:    "Ian",
    Subject: "re: Men in Black 2",
    Body:    "I thought it was pretty good",
  }

  // schedule the job to run on the "hi_priority" queue
  if err := goku.Run(job, "hi_priority"); err != nil {
    panic("will probably won't get this...")
  }
}

```

how to execute jobs
---------

```go
package main

import (
  "time"

  "github.com/i/goku"

  "./jobs"
)

func main() {
  config := goku.WorkerConfig{
    NumWorkers:   1,
    Queues:       []string{"hi_priority"],
    Timeout:      time.Second,
    Hostport:     "127.0.0.1:6379",
  }

  opts := goku.WorkerPoolOptions{
    Jobs: []goku.Job{
      jobs.WriteMessageJob{},
    }
  }

  wp, err := goku.NewWorkerPool(config, opts)
  if err != nil {
    log.Fatalf("Error creating worker pool: %v", err)
  }

  // doesn't block
  wp.Start(config, jobs)

  // wait for something...

  // waits for all current jobs to finish
  wp.Stop()
}
```
