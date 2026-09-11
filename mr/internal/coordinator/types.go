package coordinator

import (
	"mr/shared"
	"time"
	"sync"
)

// COORDINATOR
type Coordinator struct {
	mu sync.Mutex
	state CoordinatorState
	counter int
	workers int
	mapTasks []*MapTask
	reduceTasks []*ReduceTask
	done bool
}

// TASKS
type MapTask struct {
	status 		TaskStatus
	workerId	shared.WorkerId
	file 			string
	timeLeft	time.Duration
}
type ReduceTask struct {
	status 		TaskStatus
	workerId	shared.WorkerId
	bucket 		int
	timeLeft	time.Duration
}

// ENUMS
type TaskStatus int
const (
	PENDING TaskStatus = iota
	RUNNING
	FINISHED
)
type CoordinatorState int
const (
	MAP CoordinatorState = iota
	REDUCE
	EXIT
)





