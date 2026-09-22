//go:build !darwin && !linux

package productionlock

import (
	"context"
	"errors"
)

func Acquire(context.Context) (func(), error) {
	return nil, errors.New("production test lock: unsupported host")
}
