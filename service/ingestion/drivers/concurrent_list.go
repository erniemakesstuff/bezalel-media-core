package drivers

import (
	"sync"
)

type ConcurrentList struct {
	list []interface{}
	lock sync.Mutex
}

func NewConcurrentList() *ConcurrentList {
	cl := &ConcurrentList{
		list: make([]interface{}, 0),
		lock: sync.Mutex{},
	}

	return cl
}

func (cl *ConcurrentList) Add(item interface{}) {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	cl.list = append(cl.list, item)
}

func (cl *ConcurrentList) Remove(index int) {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	if index >= 0 && index < len(cl.list) {
		cl.list = append(cl.list[:index], cl.list[index+1:]...)
	}
}

func (cl *ConcurrentList) Flush() {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	cl.list = make([]interface{}, 0)
}

func (cl *ConcurrentList) Get(index int) (interface{}, bool) {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	if index >= 0 && index < len(cl.list) {
		return cl.list[index], true
	}
	return nil, false
}

func (cl *ConcurrentList) Size() int {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	return len(cl.list)
}
