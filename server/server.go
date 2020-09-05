// I would refactor/ split up this file if I had more time

package main

import (
	"errors"
	"encoding/json"
	"strings"
	"net/http"
	"stream-programming-task/jobqueue"
	"strconv"
)

type newJob struct {
	Type	jobqueue.JobType	`json:"type"`
}

type enqueuedJob struct {
	ID	int64	`json:"ID"`
}

type errorObj struct {
	Msg 	string 	`json:"error"`
	Code 	int   	`json:"code"`
}

// I don't like how this is a global variable. Wouldn't do this in production, however, I'm not very familiar
// with go's http package so I'm not sure what the accepted best practice is here.
var queue *jobqueue.Queue

func badRequest(res http.ResponseWriter, msg string) {
	json.NewEncoder(res).Encode(&errorObj{msg, 400})
}

// There's probably a better way to do this but I'm not sure how to handle url
// wildcards in go. So I'm going to parse the url manually.
func parseJobsUrl(path string) (int64, bool, error) {
	if path[0] == '/' {
		path = path[1:]
	}
	split := strings.Split(path, "/")
	// either ["jobs", {job_id}]
	// or	  ["jobs", {job_id}, "conclude"]

	if len(split) > 3 || len(split) <= 1 || len(split) == 3 && split[2] != "conclude" {
		return 0, false, errors.New("Invalid url")
	} 

	jobId, err := strconv.ParseInt(split[1], 10, 64) // same as Atoi but return int64
	if err != nil {
		return 0, false, errors.New("Invalid url")

	}

	return jobId, len(split) == 3, nil // job id, /conclude, error
}

func enqueue(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		badRequest(res, "This endpoint only accepts POST requests")
		return
	}

	decoder := json.NewDecoder(req.Body)
	var jobReq newJob
	err := decoder.Decode(&jobReq)
	if err != nil || jobReq.Type != jobqueue.TIME_CRITICAL && jobReq.Type != jobqueue.NOT_TIME_CRITICAL {
		badRequest(res, "Bad POST request body")
		return
	}

	job := jobqueue.NewJob(jobReq.Type)
	id := queue.Enqueue(job)

	json.NewEncoder(res).Encode(&enqueuedJob{id})
}

// I'm unsure so I'm assuming you don't need to associate a consumer with a specific job.
// If that is required, I would do that here also.
func dequeue(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		badRequest(res, "This endpoint only accepts GET requests")
		return
	}

	job, err := queue.Dequeue(); if err != nil {
		badRequest(res, "Queue empty error")
		return
	}

	json.NewEncoder(res).Encode(job)
}

// I'm assuming a job can be concluded by any consumer and that the job doesn't necessarily need to be already in progress.
func conclude(res http.ResponseWriter, req *http.Request, jobId int64) {
	if req.Method != "PUT" {
		badRequest(res, "This endpoint only accepts PUT requests")
		return
	}

	err := queue.Conclude(jobId); if err != nil {
		badRequest(res, err.Error())
		return
	}

	// on success, doesn't return anything
}

func info(res http.ResponseWriter, req *http.Request, jobId int64) {
	if req.Method != "GET" {
		badRequest(res, "This endpoint only accepts GET requests")
		return
	}

	job, err := queue.Info(jobId); if err != nil {
		badRequest(res, err.Error())
		return
	}

	json.NewEncoder(res).Encode(job)
}

func jobsHandler(res http.ResponseWriter, req *http.Request) {
	jobId, isConclude, err := parseJobsUrl(req.URL.Path); if err != nil {
		badRequest(res, err.Error())
	}

	if isConclude {
		conclude(res, req, jobId)
	} else {
		info(res, req, jobId)
	}
}

func main() {
	queue = jobqueue.CreateQueue()

	http.HandleFunc("/jobs/enqueue", enqueue)
	http.HandleFunc("/jobs/dequeue", dequeue)
	http.HandleFunc("/jobs/", jobsHandler)
	
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
