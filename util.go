package goku

import "reflect"

type marshalledJob struct {
	N string
	A map[string]interface{}
}

func (b *Broker) queueOrDefault(qs []string) (string, error) {
	var q string
	if len(qs) == 1 {
		q = qs[0]
	} else {
		q = b.dq
	}
	if len(q) == 0 {
		return "", ErrInvalidQueue
	}
	return q, nil
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
