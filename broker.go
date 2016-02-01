package goku

import (
	"encoding/json"
	"net"
	"reflect"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Broker objects schedule jobs to be processed
type Broker struct {
	rc       redis.Conn
	registry map[string]Job
	dq       string
}

// BrokerConfig is the information needed to set up a new broker
type BrokerConfig struct {
	Hostport     string
	Timeout      time.Duration
	DefaultQueue string
}

// NewBroker returns a new *Broker.
func NewBroker(cfg BrokerConfig) (*Broker, error) {
	conn, err := net.Dial("tcp", cfg.Hostport)
	if err != nil {
		return nil, err
	}

	return &Broker{
		rc:       redis.NewConn(conn, cfg.Timeout, cfg.Timeout),
		registry: make(map[string]Job),
		dq:       cfg.DefaultQueue,
	}, nil
}

// Run schedules jobs to be run asynchronously. If queue is not specified, the
// job will be schedules on the default queue.
func (b *Broker) Run(job Job, queue ...string) error {
	qName, err := b.queueOrDefault(queue)
	if err != nil {
		return err
	}

	args := make(map[string]interface{})

	rv := reflect.ValueOf(job)
	rt := reflect.TypeOf(job)

	for rv.Kind() == reflect.Ptr {
		return ErrPointer
	}

	for i := 0; i < rv.NumField(); i++ {
		field := rt.Field(i)
		value := rv.Field(i)
		args[field.Name] = value.Interface()
	}

	jsn, err := json.Marshal(marshalledJob{
		N: job.Name(),
		A: args,
	})
	if err != nil {
		return err
	}

	if _, err := b.rc.Do("RPUSH", qName, jsn); err != nil {
		return err
	}
	return nil
}
