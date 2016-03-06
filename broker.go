package goku

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// Broker objects schedule jobs to be processed
type Broker struct {
	registry  map[string]Job
	dq        string
	redisPool *redis.Pool
}

// BrokerConfig is the information needed to set up a new broker
type BrokerConfig struct {
	Hostport     string
	Password     string
	Timeout      time.Duration
	DefaultQueue string
}

// NewBroker returns a new *Broker.
func NewBroker(cfg BrokerConfig) (*Broker, error) {
	redisPool, err := newRedisPool(cfg.Hostport, cfg.Password, cfg.Timeout)
	if err != nil {
		return nil, err
	}

	if cfg.DefaultQueue == "" {
		return nil, ErrNoDefaultQueue
	}

	return &Broker{
		redisPool: redisPool,
		registry:  make(map[string]Job),
		dq:        cfg.DefaultQueue,
	}, nil
}

// Run schedules jobs to be run asynchronously. If queue is not specified, the
// job will be schedules on the default queue.
func (b *Broker) Run(job Job, opts ...JobOption) error {
	var jo jobOptions
	for _, opt := range opts {
		opt.f(&jo)
	}

	jsn, err := marshalJob(job)
	if err != nil {
		return err
	}

	conn := b.redisPool.Get()
	defer conn.Close()

	if _, err := conn.Do("RPUSH", b.queueOrDefault(jo.queue), jsn); err != nil {
		return err
	}
	return nil
}

func (b *Broker) RunAt(job Job, t time.Time, opts ...JobOption) error {
	var jo jobOptions
	for _, opt := range opts {
		opt.f(&jo)
	}

	jsn, err := marshalJob(job)
	if err != nil {
		return err
	}

	conn := b.redisPool.Get()
	defer conn.Close()

	queue := scheduledQueue(b.queueOrDefault(jo.queue))
	if _, err := conn.Do("ZADD", queue, t.UTC().Unix(), jsn); err != nil {
		return err
	}
	return nil
}
