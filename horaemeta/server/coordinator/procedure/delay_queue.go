/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package procedure

import (
	"container/heap"
	"sync"
	"time"
)

type procedureScheduleEntry struct {
	procedure Procedure
	runAfter  time.Time
}

type DelayQueue struct {
	maxLen int

	// This lock is used to protect the following fields.
	lock      sync.RWMutex
	heapQueue *heapPriorityQueue
	// existingProcs is used to record procedures has been pushed into the queue,
	// and they will be used to verify the addition of duplicate elements.
	existingProcs map[uint64]struct{}
}

// heapPriorityQueue is no internal lock,
// and its thread safety is guaranteed by the external caller.
type heapPriorityQueue struct {
	procedures []*procedureScheduleEntry
}

func (q *heapPriorityQueue) Len() int {
	return len(q.procedures)
}

// The dequeue order of elements is determined by the less method.
// When return procedures[i].runAfter < procedures[j].runAfter, the element with smallest will be pop first.
func (q *heapPriorityQueue) Less(i, j int) bool {
	return q.procedures[i].runAfter.Before(q.procedures[j].runAfter)
}

func (q *heapPriorityQueue) Swap(i, j int) {
	q.procedures[i], q.procedures[j] = q.procedures[j], q.procedures[i]
}

func (q *heapPriorityQueue) Push(x any) {
	item := x.(*procedureScheduleEntry)
	q.procedures = append(q.procedures, item)
}

func (q *heapPriorityQueue) Pop() any {
	length := len(q.procedures)
	if length == 0 {
		return nil
	}
	item := q.procedures[length-1]
	q.procedures = q.procedures[:length-1]
	return item
}

func (q *heapPriorityQueue) Peek() any {
	length := len(q.procedures)
	if length == 0 {
		return nil
	}
	item := q.procedures[0]
	return item
}

func NewProcedureDelayQueue(maxLen int) *DelayQueue {
	return &DelayQueue{
		maxLen: maxLen,

		lock:          sync.RWMutex{},
		heapQueue:     &heapPriorityQueue{procedures: []*procedureScheduleEntry{}},
		existingProcs: map[uint64]struct{}{},
	}
}

func (q *DelayQueue) Len() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.heapQueue.Len()
}

func (q *DelayQueue) Push(p Procedure, delay time.Duration) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.heapQueue.Len() >= q.maxLen {
		return ErrQueueFull.WithMessagef("queue max length is %d", q.maxLen)
	}

	if _, exists := q.existingProcs[p.ID()]; exists {
		return ErrPushDuplicatedProcedure.WithMessagef("procedure has been pushed, %v", p)
	}

	heap.Push(q.heapQueue, &procedureScheduleEntry{
		procedure: p,
		runAfter:  time.Now().Add(delay),
	})
	q.existingProcs[p.ID()] = struct{}{}

	return nil
}

func (q *DelayQueue) Pop() Procedure {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.heapQueue.Len() == 0 {
		return nil
	}

	entry := q.heapQueue.Peek().(*procedureScheduleEntry)
	if time.Now().Before(entry.runAfter) {
		return nil
	}

	heap.Pop(q.heapQueue)
	delete(q.existingProcs, entry.procedure.ID())

	return entry.procedure
}
