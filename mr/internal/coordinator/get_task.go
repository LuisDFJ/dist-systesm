package coordinator

import (
	"mr/shared"
)

func (c *Coordinator) GetTask( arg shared.ArgGetTask, res *shared.ResGetTask ) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	retry := true
	for retry {
		retry = false
		switch c.state {
			case MAP:
				retry = c.assignMapTask( res, arg.Id )
			case REDUCE:
				retry = c.assignReduceTask( res, arg.Id )
			case EXIT:
				c.assignExitTask( res, arg.Id )
		}
	}
	log( fmt.Sprintf("Assigning Work [type: %d, id: %d]", res.State, res.Params.Id ) )
	return nil
}

func (c *Coordinator) assignMapTask( res *shared.ResGetTask, id shared.WorkerId ) bool {
	for taskId := range c.mapTasks {
		switch c.mapTasks[taskId].status {
			case PENDING:
			case RUNNING:
			case FINISHED:
		}
	}
	return false
}

func (c *Coordinator) assignReduceTask( res *shared.ResGetTask, id shared.WorkerId ) bool {
	for taskId := range c.mapTasks {
		switch c.mapTasks[taskId].status {
			case PENDING:
			case RUNNING:
			case FINISHED:
		}
	}
	return false
}

func (c *Coordinator) assignExitTask( res *shared.ResGetTask, id shared.WorkerId ) bool {
	res.State = shared.EXIT
	return false
}

func (c *Coordinator) changeReduceTaskStatus(taskId shared.TaskId, status TaskStatus, workerId shared.WorkerId) {
	id := int(taskId)
	switch status {
	case PENDING:
		c.reduceTasks[id].status = PENDING
		c.reduceTasks[id].workerId = 0
		c.reduceTasks[id].timeLeft = 0
	case RUNNING:
		c.reduceTasks[id].status = RUNNING
		c.reduceTasks[id].workerId = workerId
		c.reduceTasks[id].timeLeft = 10 * time.Second
	case FINISHED:
		c.reduceTasks[id].status = FINISHED
		c.reduceTasks[id].workerId = 0
		c.reduceTasks[id].timeLeft = 0
	}
}

func (c *Coordinator) changeMapTaskStatus(taskId shared.TaskId, status TaskStatus, workerId shared.WorkerId) {
	id := int(taskId)
	switch status {
	case PENDING:
		c.mapTasks[id].status = PENDING
		c.mapTasks[id].workerId = 0
		c.mapTasks[id].timeLeft = 0
	case RUNNING:
		c.mapTasks[id].status = RUNNING
		c.mapTasks[id].workerId = workerId
		c.mapTasks[id].timeLeft = 10 * time.Second
	case FINISHED:
		c.mapTasks[id].status = FINISHED
		c.mapTasks[id].workerId = 0
		c.mapTasks[id].timeLeft = 0
	}
}

