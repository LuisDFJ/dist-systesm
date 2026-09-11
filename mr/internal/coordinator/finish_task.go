package coordinator

import (
	"fmt"
	"mr/shared"
)

func (c *Coordinator) FinishTask( arg shared.ArgFinishTask, res *shared.ResFinishTask ) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Conclude Task by Type and Id
	// Skip if is already finished
	switch arg.Type {
		case shared.MAP:
			// Fail if Work Collides with a Finished One
			if c.mapTasks[arg.Id].status == FINISHED {
				c.Log( fmt.Sprintf("Colliding Worker for MapTask %v", arg.Id) )
				return fmt.Errorf("work collision detected")
			}
			// Accept Task
			c.mapTasks[arg.Id].timeLeft = 0
			c.mapTasks[arg.Id].workerId = 0
			c.mapTasks[arg.Id].status = FINISHED
		case shared.REDUCE:
			// Fail if Work Collides with a Finished One
			if c.reduceTasks[arg.Id].status == FINISHED {
				c.Log( fmt.Sprintf("Colliding Worker for ReduceTask %v", arg.Id) )
				return fmt.Errorf("work collision detected")
			}
			// Accept Task
			c.reduceTasks[arg.Id].timeLeft = 0
			c.reduceTasks[arg.Id].workerId = 0
			c.reduceTasks[arg.Id].status = FINISHED
	}

	c.Log( fmt.Sprintf("Finishing Work [type: %d, id: %d]", arg.Type, arg.Id) )
	return nil
}
