package jobs

import "fmt"

type SayHelloJob struct {
	Recipient string
}

func (j SayHelloJob) Execute() error {
	fmt.Printf("Hello, %s\n", j.Name)
	return nil
}

func (j SayHelloJob) Name() string {
	return "say_hello_v0"
}
