package goku

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// redis commands (prevent typos)
const (
	blpop            = "BLPOP"
	rpush            = "RPUSH"
	zremrangebyscore = "ZREMRANGEBYSCORE"
	zrangebyscore    = "ZRANGEBYSCORE"
)

// FailureFunc is a function that gets executed when a job fails. It will get
// run when a job returns an error or panics.
type FailureFunc func(worker int, job Job, r interface{})

// WorkerConfig describes the configuration needed for setting up a new worker
// pool.
type WorkerConfig struct {
	NumWorkers int           // number of workers that belong to the pool
	Queues     []string      // what queues to pull jobs from
	Hostport   string        // redis hostport
	Password   string        // redis auth password (optional)
	Timeout    time.Duration // redis timeout

	// If a worker doesn't know how to handle a job it will be requeued.
	// sometimes requeuing can fail. This field is max number of retries before
	// losing the job.
	RequeRetries int
}

// WorkerPool is what will pull jobs from redis and distribute them to workers
// within the pool.
type WorkerPool struct {
	queues         []string
	redisPool      *redis.Pool
	fail           FailureFunc
	workCh         chan qj
	requeueMap     map[string]chan []byte
	killCh         chan struct{}
	numWorkers     int
	requeueRetries int
	registry       map[string]Job
	timeout        time.Duration
	wg             sync.WaitGroup
	m              sync.RWMutex
	running        bool
}

// WorkerPoolOptions exists for defining things that wouldn't be possible
// within a yaml configuration file. Failure is optional, but jobs are required
// if you want the workers to do anything.
type WorkerPoolOptions struct {
	Failure FailureFunc
	Jobs    []Job
}

// NewWorkerPool returns a new WorkerPool. It fails when a connection to redis
// cannot be established.
func NewWorkerPool(cfg WorkerConfig, opts WorkerPoolOptions) (*WorkerPool, error) {
	redisPool, err := newRedisPool(cfg.Hostport, cfg.Password, cfg.Timeout)
	if err != nil {
		return nil, err
	}

	wp := &WorkerPool{
		queues:         cfg.Queues,
		redisPool:      redisPool,
		workCh:         make(chan qj),
		requeueMap:     make(map[string]chan []byte),
		numWorkers:     cfg.NumWorkers,
		registry:       make(map[string]Job),
		fail:           opts.Failure,
		timeout:        cfg.Timeout,
		requeueRetries: cfg.RequeRetries,
	}

	for _, job := range opts.Jobs {
		wp.registry[job.Name()] = job
	}

	return wp, nil
}

// Start tells the worker pool to start pulling things off the queue to be
// processed.
func (wp *WorkerPool) Start() {
	wp.m.Lock()
	wp.running = true
	wp.killCh = make(chan struct{})

	for i := 0; i < wp.numWorkers; i++ {
		go wp.startWorker(i)
	}

	for _, q := range wp.queues {
		wp.requeueMap[q] = make(chan []byte)
		go wp.startReqeuer(q)
	}

	go wp.startPolling()
	go wp.startZPolling()
}

func (wp *WorkerPool) startPolling() {
	qstr := strings.Join(wp.queues, " ") // standard queues

	for wp.running {
		conn := wp.redisPool.Get()
		res, err := redis.ByteSlices(conn.Do(blpop, qstr, wp.timeout.Seconds()))
		conn.Close()
		if err != nil {
			continue
		}
		wp.workCh <- qj{string(res[0]), res[1]}
	}
}

func (wp *WorkerPool) startZPolling() {
	var zqstrs []string
	for _, qname := range wp.queues {
		zqstrs = append(zqstrs, scheduledQueue(qname))
	}

	for wp.running {
		now := time.Now().UTC().Unix()
		for _, zset := range zqstrs {
			conn := wp.redisPool.Get()
			res, err := redis.ByteSlices(conn.Do(zrangebyscore, zset, 0, now))
			conn.Close()
			if err != nil {
				continue
			}

			for _, jsn := range res {
				wp.workCh <- qj{zset, jsn}
				conn := wp.redisPool.Get()
				_, err := conn.Do(zremrangebyscore, zset, 0, now)
				if err != nil {
					// TODO -- try again
				}
				conn.Close()
			}
		}
	}
}

// Stop waits for all jobs to finish executing, and then returns.
func (wp *WorkerPool) Stop() {
	wp.running = false
	close(wp.killCh)
	wp.wg.Wait()
	wp.m.Unlock()
}

type qj struct {
	queue string
	jsn   []byte
}

func (wp *WorkerPool) startWorker(n int) {
	for {
		select {
		case <-wp.killCh:
			return
		case qj := <-wp.workCh:
			job, err := wp.getJob(qj.jsn)
			if err != nil {
				wp.requeueMap[qj.queue] <- qj.jsn
				continue
			}
			wp.doWork(job, n)
		}
	}
}

func (wp *WorkerPool) doWork(job Job, n int) {
	if wp.fail != nil {
		defer func() {
			if r := recover(); r != nil {
				wp.fail(n, job, r)
			}
		}()
	}

	wp.wg.Add(1)
	defer wp.wg.Done()
	if err := job.Execute(); err != nil {
		wp.fail(n, job, err)
	}
}

// getJob converts a json payload into a a Job with populated fields
func (wp *WorkerPool) getJob(jsn []byte) (Job, error) {
	var j marshalledJob
	if err := json.Unmarshal(jsn, &j); err != nil {
		return nil, err
	}

	emptyJob, ok := wp.registry[j.N]
	if !ok {
		return nil, ErrInvalidJob
	}

	rt := reflect.TypeOf(emptyJob)
	nj := reflect.New(rt).Elem()

	for k, v := range j.A {
		field := nj.FieldByName(k)
		if field.CanSet() {
			if f, ok := v.(float64); ok {
				v = convertFloat(field.Kind(), f)
			}
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
			wp.wg.Add(1)
			for i := 0; i < wp.requeueRetries; i++ {
				conn := wp.redisPool.Get()
				_, err := conn.Do(rpush, qn, jsn)
				conn.Close()
				if err == nil {
					break
				}
			}
			wp.wg.Done()
		}
	}
}
