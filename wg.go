package main

import (
	"fmt"
	"sync"
)

type WaitGroupExecutor struct {
	mutex sync.Mutex
	sync.WaitGroup
	errs []error
}

func (wg *WaitGroupExecutor) Run(fn func() error) {
	go func() {
		defer wg.Done()
		err := fn()
		if err == nil {
			return
		}
		wg.mutex.Lock()
		defer wg.mutex.Unlock()
		wg.errs = append(wg.errs, err)
	}()
}

func (wg *WaitGroupExecutor) Err() error {
	if len(wg.errs) == 0 {
		return nil
	}

	return fmt.Errorf("%v", wg.errs)
}
