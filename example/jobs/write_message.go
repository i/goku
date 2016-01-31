package jobs

import (
	"errors"
	"fmt"
	"os"
)

var ErrInvalidArgs = errors.New("invalid args")

type WriteMessageJob struct {
	Name    string
	Message string
}

func (j WriteMessageJob) Execute() error {
	f, err := os.Create(fmt.Sprintf("message_to_%s.txt", j.Name))
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write([]byte(j.Message)); err != nil {
		return err
	}

	return nil
}
