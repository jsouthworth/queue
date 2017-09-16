package queue

import (
	"math/big"
	"sync"
)

const (
	dropWhenFull = iota
	stopWhenFull
	coalesceWhenFull
)

var (
	bigZero = big.NewInt(0)
	bigOne  = big.NewInt(1)
)

type Queue interface {
	Enqueue(item interface{})
	TryEnqueue(item interface{}) (success bool)
	Dequeue() (item interface{})
	TryDequeue() (item interface{}, success bool)
	Close()
}

type coalescedQueue struct {
	cond    *sync.Cond
	value   interface{}
	closed  bool
	updated bool
}

func NewCoalesced() Queue {
	return &coalescedQueue{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (q *coalescedQueue) isClosed() bool {
	return q.closed
}
func (q *coalescedQueue) Enqueue(item interface{}) {
	q.TryEnqueue(item)
	return
}
func (q *coalescedQueue) TryEnqueue(item interface{}) bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.isClosed() {
		return false
	}
	defer q.cond.Signal()
	q.value = item
	q.updated = true
	return true
}
func (q *coalescedQueue) Dequeue() (item interface{}) {
	val, _ := q.dequeue(true)
	return val
}
func (q *coalescedQueue) TryDequeue() (interface{}, bool) {
	return q.dequeue(false)
}
func (q *coalescedQueue) dequeue(block bool) (interface{}, bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for !q.updated {
		if block && !q.closed {
			q.cond.Wait()
		} else {
			return nil, false
		}
	}
	q.updated = false
	return q.value, true
}
func (q *coalescedQueue) Close() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.closed = true
}

type unboundedQueue struct {
	closed bool
	cond   *sync.Cond
	head   *list
	tail   *list
	length *big.Int
}

func NewUnbounded() Queue {
	return &unboundedQueue{
		cond:   sync.NewCond(&sync.Mutex{}),
		length: big.NewInt(0),
	}
}

func (q *unboundedQueue) Enqueue(item interface{}) {
	q.TryEnqueue(item)
}

func (q *unboundedQueue) TryEnqueue(item interface{}) bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.closed {
		return false
	}
	defer q.cond.Signal()
	defer func() {
		q.length = q.length.Add(q.length, bigOne)
	}()
	n := newList(item)
	if q.length.Cmp(bigZero) == 0 {
		q.head, q.tail = n, n
		return true
	}
	q.tail = q.tail.Append(n)
	return true
}

func (q *unboundedQueue) Dequeue() interface{} {
	value, _ := q.dequeue(true)
	return value
}

func (q *unboundedQueue) TryDequeue() (interface{}, bool) {
	return q.dequeue(false)
}

func (q *unboundedQueue) Close() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.closed = true
}

func (q *unboundedQueue) dequeue(block bool) (interface{}, bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.length.Cmp(bigZero) == 0 {
		if block && !q.closed {
			q.cond.Wait()
		} else {
			return nil, false
		}
	}
	out := q.head.Item()
	q.head = q.head.Next()
	q.length = q.length.Sub(q.length, bigOne)
	return out, true
}

type boundedQueue struct {
	mu     sync.RWMutex
	closed bool
	ch     chan interface{}
}

func NewBounded(len int) Queue {
	return &boundedQueue{
		ch: make(chan interface{}, len),
	}
}

func (q *boundedQueue) Enqueue(item interface{}) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return
	}
	select {
	case q.ch <- item:
	default:
	}
}

func (q *boundedQueue) TryEnqueue(item interface{}) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return false
	}
	select {
	case q.ch <- item:
		return true
	default:
		return false
	}
}

func (q *boundedQueue) Dequeue() interface{} {
	return <-q.ch
}

func (q *boundedQueue) TryDequeue() (interface{}, bool) {
	select {
	case val, ok := <-q.ch:
		return val, ok
	default:
		return nil, false
	}
}

func (q *boundedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.ch)
}

type list struct {
	item interface{}
	next *list
}

func newList(item interface{}) *list {
	return &list{item: item}
}

func (l *list) Item() interface{} {
	return l.item
}

func (l *list) Next() *list {
	return l.next
}

func (l *list) Append(next *list) *list {
	if l.next == nil {
		l.next = next
		return l.next
	}
	return l.next.Append(next)
}
