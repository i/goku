package goku

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type FailureFunc func(worker int, job Job, r interface{})

type WorkerConfig struct {
	NumWorkers int
	Queues     []string
	Hostport   string
	Password   string
	Timeout    time.Duration
}

type WorkerPool struct {
	queues     []string
	redisPool  *redis.Pool
	fail       FailureFunc
	workCh     chan qj
	requeueMap map[string]chan []byte
	killCh     chan struct{}
	numWorkers int
	registry   map[string]Job
	timeout    time.Duration
}

type WorkerPoolOptions struct {
	Failure FailureFunc
	Jobs    []Job
}

func newRedisPool(hostport, password string, timeout time.Duration) (*redis.Pool, error) {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", hostport)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}

	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("SETEX", "FOO", 3, "BAR")
	if err != nil {
		return nil, ErrNoRedis
	}
	return pool, nil
}

func NewWorkerPool(cfg WorkerConfig, opts WorkerPoolOptions) (*WorkerPool, error) {
	redisPool, err := newRedisPool(cfg.Hostport, cfg.Password, cfg.Timeout)
	if err != nil {
		return nil, err
	}

	wp := &WorkerPool{
		queues:     cfg.Queues,
		redisPool:  redisPool,
		workCh:     make(chan qj),
		requeueMap: make(map[string]chan []byte),
		killCh:     make(chan struct{}),
		numWorkers: cfg.NumWorkers,
		registry:   make(map[string]Job),
		fail:       opts.Failure,
		timeout:    cfg.Timeout,
	}

	for _, job := range opts.Jobs {
		wp.registry[job.Name()] = job
	}

	for _, q := range wp.queues {
		wp.requeueMap[q] = make(chan []byte)
		go wp.startReqeuer(q)
	}

	return wp, nil
}

func (wp *WorkerPool) Work() error {
	for i := 0; i < wp.numWorkers; i++ {
		go wp.startWorker(i)
	}

	qstr := strings.Join(wp.queues, " ")
	for {
		conn := wp.redisPool.Get()
		res, err := redis.ByteSlices(conn.Do("BLPOP", qstr, wp.timeout.Seconds()))
		conn.Close()
		fmt.Println("BLPOPED", res, err)
		if err != nil {
			continue
		}

		wp.workCh <- qj{string(res[0]), res[1]}
	}
}

type qj struct {
	queue string
	jsn   []byte
}

func (wp *WorkerPool) startWorker(n int) {
	var job Job

	if wp.fail != nil {
		defer func() {
			if r := recover(); r != nil {
				wp.fail(n, job, r)
			}
		}()
	}

	for {
		fmt.Println("worker", n, "waiting")
		select {
		case <-wp.killCh:
			return
		case qj := <-wp.workCh:
			var err error
			job, err = wp.getJob(qj.jsn)
			if err != nil {
				wp.requeueMap[qj.queue] <- qj.jsn
				continue
			}

			if err := job.Execute(); err != nil {
				wp.fail(n, job, err)
			}
		}
	}
}

func (wp *WorkerPool) getJob(jsn []byte) (Job, error) {
	var j marshalledJob
	if err := json.Unmarshal(jsn, &j); err != nil {
		return nil, err
	}

	emptyJob, ok := wp.registry[j.N]
	if !ok {
		return nil, ErrInvalidJob
	}

	nj := reflect.New(reflect.TypeOf(emptyJob)).Elem()
	for k, v := range j.A {
		field := nj.FieldByName(k)
		if field.CanSet() {
			field.Set(reflect.ValueOf(v))
		}
	}

	job, ok := nj.Interface().(Job)
	if !ok {
		return nil, ErrInvalidJob
	}
	return job, nil
}

func (wp *WorkerPool) startReqeuer(qn string) {
	ch := wp.requeueMap[qn]
	for {
		select {
		case <-wp.killCh:
			return
		case jsn := <-ch:
			var i int
			for ; ; i++ {
				conn := wp.redisPool.Get()
				_, err := conn.Do("RPUSH", qn, jsn)
				conn.Close()
				if err != nil {
					if i == 10 {
						panic(err)
					}
				}
				break
			}
		}
	}
}
