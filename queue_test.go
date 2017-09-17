package queue

import (
	"sync"
	"testing"
	"time"
)

func assert(t *testing.T, cond bool, msg string) {
	if !cond {
		t.Fatal(msg)
	}
}

type queueCons func() Queue

/*
 * The testQueue* functions test the generic semantics to which
 * all queues should conform.
 */
func testQueueEnqueue(t *testing.T, cons queueCons) {
	q := cons()
	q.Enqueue(1)
	v := q.Dequeue()
	assert(t, v == 1, "Enqueue didn't insert the proper value")
}

func testQueueEnqueueOnClosed(t *testing.T, cons queueCons) {
	q := cons()
	q.Close()
	q.Enqueue(1)
	_, ok := q.TryDequeue()
	assert(t, !ok,
		"Enqueue shouldn't have happened on a closed queue")
}

func testQueueBlockingDequeue(t *testing.T, cons queueCons) {
	q := cons()
	sync := make(chan struct{})
	go func() {
		q.Dequeue()
		close(sync)
	}()
	select {
	case <-sync:
		t.Fatal("Dequeue didn't block")
	case <-time.After(100 * time.Millisecond):
	}
}

func testQueueClosedBlockingDequeue(t *testing.T, cons queueCons) {
	q := cons()
	q.Close()
	sync := make(chan struct{})
	go func() {
		q.Dequeue()
		close(sync)
	}()
	select {
	case <-sync:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Dequeue should not have blocked")
	}
}
func testQueueTryDequeue(t *testing.T, cons queueCons) {
	q := cons()
	q.Enqueue(1)
	_, ok := q.TryDequeue()
	assert(t, ok, "Dequeue should have succeeded")
}

func testQueueDequeueOrClosed(t *testing.T, cons queueCons) {
	q := cons()
	q.Enqueue(1)
	q.Close()
	v, ok := q.DequeueOrClosed()
	assert(t, ok && v == 1, "DequeueOrClosed should have returned a value")
	v, ok = q.DequeueOrClosed()
	assert(t, !ok && v == nil, "DequeueOrClosed should not have returned a value")
}

func testQueueBlockingTryDequeue(t *testing.T, cons queueCons) {
	q := cons()
	_, ok := q.TryDequeue()
	assert(t, !ok, "Dequeue should have failed")
}

func testQueueClosedBlockingTryDequeue(t *testing.T, cons queueCons) {
	q := cons()
	q.Close()
	_, ok := q.TryDequeue()
	assert(t, !ok, "Dequeue should have failed")
}

func testQueueClosedNonBlockingTryDequeue(t *testing.T, cons queueCons) {
	q := cons()
	q.Enqueue(1)
	q.Close()
	val, ok := q.TryDequeue()
	assert(t, ok && val == 1, "Dequeue should have returned a value")
}

func testQueueClosedBlockingAfterNonBlockingTryDequeue(t *testing.T, cons queueCons) {
	q := cons()
	q.Enqueue(1)
	q.Close()
	val, ok := q.TryDequeue()
	assert(t, ok && val == 1, "Dequeue should have returned a value")
	val, ok = q.TryDequeue()
	assert(t, !ok && val == nil, "Dequeue shouldn't have returned a value")
}

func testQueueBlockingDequeueFollowingEnqueue(t *testing.T, cons queueCons) {
	q := cons()
	sync := make(chan interface{})
	go func() {
		sync <- q.Dequeue()
	}()
	q.Enqueue(1)
	q.Close()
	select {
	case val := <-sync:
		assert(t, val == 1, "Dequeue should have returned a value")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Dequeue should not have blocked")
	}
}
func testQueueBlocksOnSecondDequeue(t *testing.T, cons queueCons) {
	q := cons()
	q.Enqueue(1)
	q.Dequeue()
	sync := make(chan struct{})
	go func() {
		q.Dequeue()
		close(sync)
	}()
	select {
	case <-sync:
		t.Fatal("Dequeue didn't block")
	case <-time.After(100 * time.Millisecond):
	}
}

func testQueueCloseOfClosed(t *testing.T, cons queueCons) {
	q := cons()
	q.Close()
	q.Close()
}

func testQueueTryEnqueue(t *testing.T, cons queueCons) {
	q := cons()
	res := q.TryEnqueue(1)
	assert(t, res, "TryEnqueue should have succeeded")
}

func testQueueTryEnqueueClosed(t *testing.T, cons queueCons) {
	q := cons()
	q.Close()
	res := q.TryEnqueue(1)
	assert(t, !res, "TryEnqueue should have failed")
}

func testQueueSemantics(t *testing.T, cons queueCons) {
	t.Run("Enqueue", func(t *testing.T) {
		testQueueEnqueue(t, cons)
	})
	t.Run("EnqueueOnClosed", func(t *testing.T) {
		testQueueEnqueueOnClosed(t, cons)
	})
	t.Run("BlockingDequeue", func(t *testing.T) {
		testQueueBlockingDequeue(t, cons)
	})
	t.Run("ClosedBlockingDequeue", func(t *testing.T) {
		testQueueClosedBlockingDequeue(t, cons)
	})
	t.Run("TryDequeue", func(t *testing.T) {
		testQueueTryDequeue(t, cons)
	})
	t.Run("DequeueOrClosed", func(t *testing.T) {
		testQueueDequeueOrClosed(t, cons)
	})
	t.Run("BlockingTryDequeue", func(t *testing.T) {
		testQueueBlockingTryDequeue(t, cons)
	})
	t.Run("ClosedBlockingTryDequeue", func(t *testing.T) {
		testQueueClosedBlockingTryDequeue(t, cons)
	})
	t.Run("ClosedNonBlockingTryDequeue", func(t *testing.T) {
		testQueueClosedNonBlockingTryDequeue(t, cons)
	})
	t.Run("ClosedBlockingAfterNonBlockingTryDequeue", func(t *testing.T) {
		testQueueClosedBlockingAfterNonBlockingTryDequeue(t, cons)
	})
	t.Run("BlockingDequeueFollowingEnqueue", func(t *testing.T) {
		testQueueBlockingDequeueFollowingEnqueue(t, cons)
	})
	t.Run("BlocksOnSecondDequeue", func(t *testing.T) {
		testQueueBlocksOnSecondDequeue(t, cons)
	})
	t.Run("CloseOfClosedQueue", func(t *testing.T) {
		testQueueCloseOfClosed(t, cons)
	})
	t.Run("TryEnqueue", func(t *testing.T) {
		testQueueTryEnqueue(t, cons)
	})
	t.Run("TryEnqueueClosed", func(t *testing.T) {
		testQueueTryEnqueueClosed(t, cons)
	})
}

func TestCoalescedQueueSemantics(t *testing.T) {
	testQueueSemantics(t, NewCoalesced)
}

func TestCoalescedCoalescesValues(t *testing.T) {
	q := NewCoalesced()
	q.Enqueue(1)
	q.Enqueue(10)
	q.Enqueue(20)
	val := q.Dequeue()
	assert(t, val == 20, "Dequeue should have returned last value enqueued")
}

func TestCoalescedMultipleEnqueuers(t *testing.T) {
	q := NewCoalesced()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for i := 0; i < 10; i++ {
			q.Enqueue(i)
		}
		wg.Done()
	}()
	go func() {
		for i := 10; i < 20; i++ {
			q.Enqueue(i)
		}
		wg.Done()
	}()
	wg.Wait()
	v := q.Dequeue()
	assert(t, v == 9 || v == 19, "Unexpected dequeued value")

}

func TestUnboundedQueueSemantics(t *testing.T) {
	testQueueSemantics(t, NewUnbounded)
}

func TestUnboundedIsUnbounded(t *testing.T) {
	q := NewUnbounded()
	for i := 0; i < 1000000; i++ {
		q.Enqueue(i)
	}
	for i := 0; i < 1000000; i++ {
		assert(t, q.Dequeue() == i,
			"Dequeue should have been equivalent to index")
	}
}

func TestBoundedQueueSemantics(t *testing.T) {
	testQueueSemantics(t, func() Queue {
		return NewBounded(10)
	})
}

func TestBoundedTryEnqueueWhenFull(t *testing.T) {
	q := NewBounded(10)
	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}
	ok := q.TryEnqueue(10)
	assert(t, !ok, "TryEnqueue should have failed")
}

func TestBoundedNonBlockingEnqueueWhenFull(t *testing.T) {
	q := NewBounded(10)
	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}
	sync := make(chan struct{})
	go func() {
		q.Enqueue(10)
		close(sync)
	}()
	select {
	case <-sync:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Enqueue should not have blocked")
	}

}

func TestBoundedDequeueWhenFull(t *testing.T) {
	q := NewBounded(10)
	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}
	q.Enqueue(10)
	for i := 0; i < 10; i++ {
		assert(t, q.Dequeue() == i,
			"Dequeue should have been equivalent to index")
	}
	_, ok := q.TryDequeue()
	assert(t, !ok, "TryDequeue should have failed")
}

func TestBlockingQueueSemantics(t *testing.T) {
	testQueueSemantics(t, func() Queue {
		return NewBlocking(10)
	})
}
func TestBlockingEnqueueWhenFull(t *testing.T) {
	q := NewBlocking(10)
	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}
	sync := make(chan struct{})
	go func() {
		q.Enqueue(10)
		close(sync)
	}()
	select {
	case <-sync:
		t.Fatal("Enqueue should have blocked")
	case <-time.After(100 * time.Millisecond):
	}

}

func TestList(t *testing.T) {
	li := newList(1)
	assert(t, li.Item() == 1, "List didn't return expected item")
	assert(t, li.Next() == nil, "List Next should be nil")
	li.Append(newList(2))
	li.Append(newList(3))
	li.Append(newList(4))
	assert(t, li.Next() != nil, "List Next shouldn't be nil")
	tmp := li
	for i := 1; i < 5; i++ {
		assert(t, tmp.Item() == i, "Incorrect entry at element")
		tmp = tmp.Next()
	}
}

func TestRange(t *testing.T) {
	q := NewUnbounded()
	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}
	q.Close()
	count := 0
	Range(q, func(v interface{}) {
		count += v.(int)
	})
	assert(t, count == 45, "Count didn't equal expected value")
}

func TestMove(t *testing.T) {
	q := NewUnbounded()
	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}
	q.Close()
	q2 := NewUnbounded()
	Move(q2, q)
	q2.Close()
	for i := 0; i < 10; i++ {
		assert(t, q2.Dequeue() == i, "Incorrect entry at element")
	}
}
