package goku

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
