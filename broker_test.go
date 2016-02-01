package goku

import (
	"testing"
	"time"

	"github.com/i/goku/example/jobs"
	"github.com/stretchr/testify/assert"
)

func TestBroker(t *testing.T) {
	broker, err := NewBroker(BrokerConfig{
		Hostport:     "127.0.0.1:6379",
		Timeout:      time.Second,
		DefaultQueue: "test",
	})

	assert.NoError(t, err)

	job := jobs.SayHelloJob{
		Recipient: "ian",
	}

	err = broker.Run(job)
	assert.NoError(t, err)
}
