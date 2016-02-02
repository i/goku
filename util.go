package goku

import (
	"encoding/json"
	"fmt"
	"reflect"
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

func scheduledQueue(queue string) string {
	return fmt.Sprintf("z:%s", queue)
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

	// test the connection
	_, err := conn.Do("SETEX", "FOO", 3, "BAR")
	if err != nil {
		return nil, ErrNoRedis
	}
	return pool, nil
}
