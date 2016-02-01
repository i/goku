package jobs

import (
	"errors"
	"fmt"
	"os"
)

var ErrInvalidArgs = errors.New("invalid args")

type WriteMessageJob struct {
	To      string
	Message string
}

func (j WriteMessageJob) Execute() error {
	f, err := os.Create(fmt.Sprintf("message_to_%s.txt", j.To))
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write([]byte(j.Message)); err != nil {
		return err
	}

	return nil
}

func (j WriteMessageJob) Name() string {
	return "write_messag_job_v0"
}
