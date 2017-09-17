//Package queue implements a set of synchronization queues and some generic
//algorithms that will operate on these queues.
//The queues are similar to go channels in purpose but allow for
//extensible semantics when the synchronization pattern provided by channels
//does not naturally fit the problem domain.
package queue

import (
	"math/big"
	"sync"
)

var (
	bigZero = big.NewInt(0)
	bigOne  = big.NewInt(1)
)

//The Queue interface represents the abstract notion of a synchronization
//queue. Queues have a relatively small interface with some specific
//semantics that need to be accounted for.
type Queue interface {
	//Enqueue adds an item to the end of the queue.
	//It is safe to Enqueue on a closed queue.
	Enqueue(item interface{})
	//TryEnqueue adds an item and returns whether the act was successful
	TryEnqueue(item interface{}) (success bool)
	//Dequeue blocks until an item is in the queue then returns that item
	//unless the queue is closed in which case it returns nil.
	Dequeue() (item interface{})
	//TryDequeue tries to dequeue an item and returns whether
	//it was successful it may be unsuccessful if the queue was empty
	//or closed.
	TryDequeue() (item interface{}, success bool)
	//DequeueOrClosed returns an item or will return that the queue
	//is closed.
	DequeueOrClosed() (item interface{}, closed bool)
	//Close closes a queue, which creates a sentinal value that will
	//always be returned when the queue is closed. It is safe to
	//close a queue more than once.
	Close()
}

//Range calls the fn on each enqueued item until the queue is closed
func Range(q Queue, fn func(item interface{})) {
	for i, ok := q.DequeueOrClosed(); ok; i, ok = q.DequeueOrClosed() {
		fn(i)
	}
}

//Move moves items from one queue to another. The input queue must be
//closed prior to calling Move otherwise Move will loop forever reenqueuing
//items.
func Move(out Queue, in Queue) {
	Range(in, func(v interface{}) {
		out.Enqueue(v)
	})
}

type coalescedQueue struct {
	cond    *sync.Cond
	value   interface{}
	closed  bool
	updated bool
}

//A coalesced queue, is useful when one does not care about missed updates.
//That is specific cases where the last value in is what matters, not the
//interveaning values. This can be used to notify another process when a
//subscribed value changes, but to account for cases where the consumer is
//slower than the value changes and the last change before the consumer
//catches up has enough information for it to continue.
//These semantics will not always be useful but are what is desired
//in some scenarios.
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
func (q *coalescedQueue) DequeueOrClosed() (interface{}, bool) {
	return q.dequeue(true)
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

//An unbounded queue, will grow without bounds. This is useful in unpredictable
//bursty scenarios. The only feedback mechanism for this queue is memory
//pressure. Caution should be taken when using an unbounded queue.
//If the producer constantly overruns the consumer then the queue will never
//drain.
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

func (q *unboundedQueue) DequeueOrClosed() (interface{}, bool) {
	return q.dequeue(true)
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

//A bounded queue has the semantics of a go channel that drops
//on enqueue when full.
func NewBounded(limit int) Queue {
	return newBounded(limit)
}

func newBounded(limit int) *boundedQueue {
	return &boundedQueue{
		ch: make(chan interface{}, limit),
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
func (q *boundedQueue) DequeueOrClosed() (interface{}, bool) {
	val, ok := <-q.ch
	return val, ok
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

type blockingQueue struct {
	*boundedQueue
}

//A blocking queue has the same semantics as a go channel.
func NewBlocking(limit int) Queue {
	return &blockingQueue{
		boundedQueue: newBounded(limit),
	}
}

func (q *blockingQueue) Enqueue(item interface{}) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return
	}
	q.ch <- item
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
