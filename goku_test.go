package goku

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestJob struct {
	Foo int
	Bar string
}

func (tj TestJob) Name() string {
	return "test_job"
}

var tjWasCalled bool

func (tj TestJob) Execute() error {
	fmt.Println("ok")
	tjWasCalled = true
	return nil
}

func TestBroker(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	hostport := "127.0.0.1:6379"
	queueName := "goku_test"

	broker, err := NewBroker(BrokerConfig{
		Hostport:     hostport,
		Timeout:      time.Second,
		DefaultQueue: queueName,
	})

	require.NoError(err)

	job := TestJob{
		Foo: 4,
		Bar: "sup",
	}

	err = broker.Run(job)
	assert.NoError(err)

	conn, err := redis.Dial("tcp", hostport)
	require.NoError(err)

	jsn, err := redis.Bytes(conn.Do("LPOP", queueName))
	assert.NoError(err)

	var m map[string]interface{}
	json.Unmarshal(jsn, &m)
	args := m["A"].(map[string]interface{})

	assert.Equal(m["N"], job.Name())
	assert.Equal(args["Foo"], float64(4))
	assert.Equal(args["Bar"], "sup")
}

func TestWorker(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	queue := "goku_test"
	hostport := "127.0.0.1:6379"

	config := WorkerConfig{
		NumWorkers: 1,
		Queues:     []string{queue},
		Hostport:   hostport,
		Timeout:    time.Second,
	}

	opts := WorkerPoolOptions{
		Failure: nil,
		Jobs: []Job{
			TestJob{},
		},
	}

	// start the worker
	wp, err := NewWorkerPool(config, opts)
	assert.NoError(err)
	wp.Start()

	tjWasCalled = false

	// schedule the job from the broker
	broker, err := NewBroker(BrokerConfig{
		Hostport:     hostport,
		Timeout:      time.Second,
		DefaultQueue: queue,
	})

	require.NoError(err)

	job := TestJob{
		Foo: 4,
		Bar: "sup",
	}

	err = broker.Run(job)
	assert.NoError(err)
	time.Sleep(time.Second)
	fmt.Println("HI")
	wp.Stop()
}
