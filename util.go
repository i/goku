package goku

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type marshalledJob struct {
	N string
	A map[string]interface{}
}

func (b *Broker) queueOrDefault(q string) string {
	if q == "" {
		return b.dq
	}
	return q
}

func convertFloat(kind reflect.Kind, f float64) interface{} {
	switch kind {
	case reflect.Int:
		return int(f)
	case reflect.Int8:
		return int8(f)
	case reflect.Int16:
		return int16(f)
	case reflect.Int32:
		return int32(f)
	case reflect.Int64:
		return int64(f)
	case reflect.Uint:
		return uint(f)
	case reflect.Uint8:
		return uint8(f)
	case reflect.Uint16:
		return uint16(f)
	case reflect.Uint32:
		return uint32(f)
	case reflect.Uint64:
		return uint64(f)
	case reflect.Uintptr:
		return uintptr(f)
	case reflect.Float32:
		return float32(f)
	case reflect.Float64:
		return f
	default:
		return 0
	}
}

func marshalJob(job Job) ([]byte, error) {
	rv := reflect.ValueOf(job)
	rt := reflect.TypeOf(job)
	for rv.Kind() == reflect.Ptr {
		return nil, ErrPointer
	}

	args := make(map[string]interface{})

	for i := 0; i < rv.NumField(); i++ {
		field := rt.Field(i)
		value := rv.Field(i)
		args[field.Name] = value.Interface()
	}

	return json.Marshal(marshalledJob{N: job.Name(), A: args})
}

func scheduledQueueKey(queue string) string {
	return fmt.Sprintf("z:%s", queue)
}

func cronTabKey(queue string) string {
	return fmt.Sprintf("h:%s", queue)
}

func newPool(hostport, password string, timeout time.Duration) (*pool, error) {
	p := redis.Pool{
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

	conn := p.Get()
	defer conn.Close()

	// test the connection
	_, err := conn.Do("SETEX", "FOO", 3, "BAR")
	if err != nil {
		return nil, ErrNoRedis
	}
	return &pool{
		Pool: p,
	}, nil
}

type pool struct {
	redis.Pool
	m sync.Mutex
}

type lock struct {
	*pool
	key string
}

func (l *lock) release() error {
	conn := l.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", l.key)
	return err
}

func (p *pool) getLock(key string) (*lock, error) {
	p.m.Lock()
	defer p.m.Unlock()

	conn := p.Pool.Get()
	defer conn.Close()

	_, err := redis.String(conn.Do("SET", key, "LOCK", "NX"))
	if err != nil {
		return nil, err
	}
	return &lock{p, key}, nil
}

func getJobUUID(jsn []byte) string {
	return fmt.Sprintf("%x", md5.Sum(jsn))
}

type cronEntry struct {
	JobUUID  string
	Interval time.Duration
	Last     time.Time
}
