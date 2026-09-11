package coordinator

import (
	"fmt"
	"mr/shared"
	"time"
)

func (c *Coordinator) GetTask( arg shared.ArgGetTask, res *shared.ResGetTask ) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Try Once
	retry := true
	for retry {
		retry = false
		switch c.state {
			case MAP: // Assign Map Task Else Retry with Reduce Task
				retry = c.assignMapTask( res, arg.Id )
				if retry { c.state = REDUCE }
			case REDUCE: // Assign Reduce Task Else Retry with Exit Task
				retry = c.assignReduceTask( res, arg.Id )
				if retry { c.state = EXIT }
			case EXIT: // Exit all Workers
				c.assignExitTask( res )
		}
	}
	c.Log( fmt.Sprintf("Assigning Work [type: %d, id: %d]", res.State, res.Params.Id ) )
	return nil
}

func (c *Coordinator) assignMapTask( res *shared.ResGetTask, id shared.WorkerId ) bool {
	// If all tasks are finished proceed with Reduce Tasks
	flag := true
	for taskId := range c.mapTasks {
		switch c.mapTasks[taskId].status {
			case PENDING:
				// Assign Task to Worker
				res.State = shared.MAP
				res.Params.File = c.mapTasks[taskId].file
				res.Params.Id = shared.TaskId(taskId)
				// Update Task Satus and Assign 10 seconds of life
				c.mapTasks[taskId].status = RUNNING
				c.mapTasks[taskId].timeLeft = 10 * time.Second
				c.mapTasks[taskId].workerId = id
				return false
			case RUNNING:
				flag = false
			case FINISHED:
		}
	}
	// If not task assigned and not retry, send IDLE
	if !flag {
		res.State = shared.IDLE
	}
	return flag
}

func (c *Coordinator) assignReduceTask( res *shared.ResGetTask, id shared.WorkerId ) bool {
	// If all tasks are finished proceed with Exit Tasks
	flag := true
	for taskId := range c.reduceTasks {
		switch c.reduceTasks[taskId].status {
			case PENDING:
				// Assign Task to Worker
				res.State = shared.REDUCE
				res.Params.Bucket = c.reduceTasks[taskId].bucket
				res.Params.Id = shared.TaskId(taskId)
				// Update Task Satus and Assign 10 seconds of life
				c.reduceTasks[taskId].status = RUNNING
				c.reduceTasks[taskId].timeLeft = 10 * time.Second
				c.reduceTasks[taskId].workerId = id
				return false
			case RUNNING:
				flag = false
			case FINISHED:
		}
	}
	// If not task assigned and not retry, send IDLE
	if !flag {
		res.State = shared.IDLE
	}
	return flag
}

func (c *Coordinator) assignExitTask( res *shared.ResGetTask ) bool {
	res.State = shared.EXIT
	return false
}
