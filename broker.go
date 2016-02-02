package goku

import "time"

// Broker objects schedule jobs to be processed
type Broker struct {
	registry     map[string]Job
	defaultQueue string
	cronTabKey   string
	pool         *pool
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
	redisPool, err := newPool(cfg.Hostport, cfg.Password, cfg.Timeout)
	if err != nil {
		return nil, err
	}

	if cfg.DefaultQueue == "" {
		return nil, ErrNoDefaultQueue
	}

	return &Broker{
		pool:         redisPool,
		registry:     make(map[string]Job),
		defaultQueue: cfg.DefaultQueue,
		cronTabKey:   cronTabKey(cfg.DefaultQueue),
	}, nil
}

// Run schedules jobs to be run asynchronously. If queue is not specified, the
// job will be schedules on the default queue.
func (b *Broker) Run(job Job, opts ...JobOptions) error {
	var jo JobOptions
	if len(opts) == 1 {
		jo = opts[0]
	}

	jsn, err := marshalJob(job)
	if err != nil {
		return err
	}

	conn := b.pool.Get()
	defer conn.Close()

	if _, err := conn.Do("RPUSH", b.queueOrDefault(jo.Queue), jsn); err != nil {
		return err
	}
	return nil
}

func (b *Broker) RunAt(job Job, t time.Time, opts ...JobOptions) error {
	var jo JobOptions
	if len(opts) == 1 {
		jo = opts[0]
	}

	jsn, err := marshalJob(job)
	if err != nil {
		return err
	}

	conn := b.pool.Get()
	defer conn.Close()

	queue := scheduledQueueKey(b.queueOrDefault(jo.Queue))
	if _, err := conn.Do("ZADD", queue, t.UTC().Unix(), jsn); err != nil {
		return err
	}
	return nil
}

func (b *Broker) RunEvery(job Job, interval time.Duration, opts ...JobOptions) error {
	var jo JobOptions
	if len(opts) == 1 {
		jo = opts[0]
	}

	jsn, err := marshalJob(job)
	if err != nil {
		return err
	}

	lock, err := b.pool.getLock(b.cronTabKey)
	if err != nil {
		return err
	}
	defer lock.release()

	conn := b.pool.Get()
	defer conn.Close()

	_, err = conn.Do("HSET", b.cronTabKey, getJobKey(jsn))
	if err != nil {
		return err
	}

	return nil
}

/*
	crontab:
		abc-123:
			last: 5min ago
			interval: 6s
			jobUUID: 4


	goku:c:123-4456:
		N:
		A:
*/

// HGETALL goku:schedule
// unmarshal map to
// {
//   jobName: { args... }
// }
// for each job:
// 		lock job
//`
//db = {
//jobName: {
//args
//}
//}

//locks = {
//}
//
