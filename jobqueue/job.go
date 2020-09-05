package jobqueue

import "sync/atomic"

// defining some enums
type JobType string
const (
	TIME_CRITICAL 		JobType = "TIME_CRITICAL"
	NOT_TIME_CRITICAL 	JobType = "NOT_TIME_CRITICAL"
)

type JobStatus string
const (
	QUEUED				JobStatus = "QUEUED"
	IN_PROGRESS			JobStatus = "IN_PROGRESS"
	CONCLUDED			JobStatus = "CONCLUDED"
)

type Job struct {
	ID 		int64
	Type 	JobType
	Status	JobStatus
}

var id int64 = 0 // In production I would consider using a uuid library or something that doesn't have overflow problems.
// Also because I'm using atomic operations to increment the id value, this limits the performance of the server.
func NewJob(jt JobType) *Job {
	atomic.AddInt64(&id, 1)
	return &Job{id, jt, QUEUED}
}
