package jobqueue

import (
	"errors"
	"sync"
)

// We ideally want fast lookups in the queue (similar to LRU Cache problem).
// We don't want to scan the whole queue looking for a job everytime /jobs/{job_id}
// is queried. Therefore, internally the queue is implemented as both a slice and a map.
type Queue struct {
	queue	[]*Job
	ids		map[int64]*Job
	current int				// current queue index
	mutex	sync.Mutex		// In production would probably use either a lock free queue, RWLock or some kind of channels solution.
							// I'm using a mutex for simplicity, correctness and because of time constraints.
}

func CreateQueue() *Queue {
	q := &Queue{}
	q.ids = make(map[int64]*Job)
	return q
}

func (q *Queue) Enqueue(j *Job) int64 {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.queue = append(q.queue, j)
	q.ids[j.ID] = j
	return j.ID
}

// There is never any physical deletion only logical deletion. This means the implementation will run out of memory eventually.
func (q *Queue) Dequeue() (*Job, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.current == len(q.queue) {
		return nil, errors.New("Queue is empty")
	}

	// iterate through queue until we find a QUEUED job (i.e. ignore COMPLETED jobs)
	front := q.queue[q.current]
	for front.Status != QUEUED  {
		q.current ++

		if q.current == len(q.queue) {
			return nil, errors.New("Queue is empty")

		}
		front = q.queue[q.current]
	}

	// update job status
	front.Status = IN_PROGRESS
	return front, nil
}

func (q *Queue) Conclude(jobId int64) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	job, found := q.ids[jobId]
	if !found {
		return errors.New("Invalid jobId")
	}
	// I'm assuming it is not an error to conclude a job even if another consumer is working on it.
	// I'm also assuming it it not an error to conclude an already concluded job.
	
	job.Status = CONCLUDED
	return nil
}

func (q *Queue) Info(jobId int64) (*Job, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	job, found := q.ids[jobId]
	if !found {
		return nil, errors.New("Invalid jobId")
	}
	return job, nil
}