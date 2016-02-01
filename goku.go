package goku

import "errors"

// generic goku errors
var (
	ErrPointer           = errors.New("method receiver was a pointer when it shouldn't be")
	ErrStdNotInitialized = errors.New("default broker hasn't been initialized")
	ErrInvalidQueue      = errors.New("invalid queue name")
	ErrNoRedis           = errors.New("can't establish a connection to redis")
	ErrInvalidJob        = errors.New("invalid job")
)

// std is the default broker
var std *Broker

// Configure configures the default broker for package level use
func Configure(cfg BrokerConfig) error {
	b, err := NewBroker(cfg)
	if err != nil {
		return err
	}
	std = b
	return nil
}

// Job is any type that implements Execute and Name. In order for a job to be
// valid, all fields used within its Execute method must be exported.
type Job interface {
	Name() string
	Execute() error
}

// Run schedules a job using the default broker. Before calling goku.Run, the
// default client must be configured using goku.Configure.
func Run(j Job, queue ...string) error {
	if std == nil {
		return ErrStdNotInitialized
	}
	return std.Run(j, queue...)
}
