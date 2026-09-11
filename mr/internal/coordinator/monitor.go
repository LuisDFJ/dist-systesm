package coordinator

import (
	"time"
)

func (c *Coordinator) Monitor( interval time.Duration, done <-chan bool ) {
	ticker := time.NewTicker( interval )
	for {
		select {
			case <-done:
				ticker.Stop()
				return
			case <-ticker.C:
				c.mu.Lock()
				switch c.state {
					case MAP:
						c.checkMapTimeLeft(interval)
					case REDUCE:
						c.checkReduceTimeLeft(interval)
				}
				c.mu.Unlock()
		}
	}
}

func (c *Coordinator) checkMapTimeLeft( interval time.Duration ) {
	dropTasks := []int{}
	// Iterate through Reduce Tasks
	for taskId := range c.mapTasks {
		if c.mapTasks[taskId].status == RUNNING {
			// Substract Interval to Time Left
			c.mapTasks[taskId].timeLeft = max( c.mapTasks[taskId].timeLeft - interval, 0 )
			if c.mapTasks[taskId].timeLeft == 0 { dropTasks = append(dropTasks, taskId) }
		}
	}
	// Reset Idle Tasks
	for _, taskId := range dropTasks {
		c.mapTasks[taskId].workerId = 0
		c.mapTasks[taskId].timeLeft = 0
		c.mapTasks[taskId].status = PENDING
	}
}

func (c *Coordinator) checkReduceTimeLeft( interval time.Duration ) {
	dropTasks := []int{}
	// Iterate through Reduce Tasks
	for taskId := range c.reduceTasks {
		if c.reduceTasks[taskId].status == RUNNING {
			// Substract Interval to Time Left
			c.reduceTasks[taskId].timeLeft = max( c.reduceTasks[taskId].timeLeft - interval, 0 )
			if c.reduceTasks[taskId].timeLeft == 0 { dropTasks = append(dropTasks, taskId) }
		}
	}
	// Reset Idle Tasks
	for _, taskId := range dropTasks {
		c.reduceTasks[taskId].workerId = 0
		c.reduceTasks[taskId].timeLeft = 0
		c.reduceTasks[taskId].status = PENDING
	}
}
