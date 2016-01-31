package goku

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"reflect"
	"runtime"
	"time"

	"github.com/garyburd/redigo/redis"
)

// goku errors
var (
	ErrPointer = errors.New("method receiver was a pointer when it shouldn't be")
)

var (
	registry map[string]Job
	rc       redis.Conn
)

type Config struct {
	Hostport string
	Timeout  time.Duration
}

func Configure(cfg Config) error {
	registry = make(map[string]Job)
	conn, err := net.Dial("tcp", cfg.Hostport)
	if err != nil {
		return err
	}
	rc = redis.NewConn(conn, cfg.Timeout, cfg.Timeout)
	return nil
}

type Job interface {
	Execute() error
}

func Register(jobs ...Job) {
	for _, j := range jobs {
		registry[getFunctionName(j.Execute)] = j
	}
}

type marshalledJob struct {
	N string
	A map[string]interface{}
}

func Run(j Job, queue string) error {
	args := make(map[string]interface{})

	rv := reflect.ValueOf(j)
	rt := reflect.TypeOf(j)

	for rv.Kind() == reflect.Ptr {
		return ErrPointer
	}

	for i := 0; i < rv.NumField(); i++ {
		field := rt.Field(i)
		value := rv.Field(i)
		args[field.Name] = value.Interface()
	}

	jsn, err := json.Marshal(marshalledJob{
		N: getFunctionName(j.Execute),
		A: args,
	})

	if err != nil {
		return err
	}

	if _, err := rc.Do("RPUSH", queue, jsn); err != nil {
		return err
	}
	return nil
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

type WorkerConfig struct {
	NumWorkers   int
	PollInterval time.Duration
	Failure      func(worker int, jobName string, r interface{})
	Queue        string
}

type FailureFunc func(worker int, jobName string, r interface{})

func Work(config WorkerConfig, jobs []Job) error {
	ch := make(chan []byte)
	requeuerCh := make(chan []byte)

	for i := 0; i < config.NumWorkers; i++ {
		go worker(i, ch, requeuerCh, config.Failure)
	}

	for ; ; time.Sleep(config.PollInterval) {
		jsn, err := redis.Bytes(rc.Do("LPOP", config.Queue))
		if err != nil {
			continue
		}
		ch <- jsn
	}
}

func reqeuer(qn string, ch chan []byte) {
	for jsn := range ch {
		if _, err := rc.Do("RPUSH", qn, jsn); err != nil {
			// TODO (retry)
		}
	}
}

func worker(n int, ch chan []byte, requeuerCh chan []byte, failure FailureFunc) {
	var jobName string

	if failure != nil {
		defer func() {
			if r := recover(); r != nil {
				failure(n, jobName, r)
			}
		}()
	}

	for jsn := range ch {
		var j marshalledJob
		if err := json.Unmarshal(jsn, &j); err != nil {
			log.Fatal(err)
		}

		jobName = j.N
		job, ok := registry[jobName]
		if !ok {
			requeuerCh <- jsn
			continue
		}

		nj := reflect.New(reflect.TypeOf(job)).Elem()
		for k, v := range j.A {
			field := nj.FieldByName(k)
			if field.CanSet() {
				field.Set(reflect.ValueOf(v))
			}
		}
		nj.Interface().(Job).Execute()
	}
}
