package queue

import "errors"

// ErrNotImplemented is returned by placeholder queue backends.
var ErrNotImplemented = errors.New("queue backend is not implemented")
