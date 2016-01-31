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
  err := goku.Configure(goku.Config{
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
    PollInterval: time.Second,
    Queue:        "hi_priority",
  }

  // this worker will only process WriteMessageJobs
  jobs := []goku.Job{
    jobs.WriteMessageJob{},
  }

  // blocks forever
  if err := goku.Work(config, jobs); err != nil {
    log.Fatal(err)
  }
}
```
