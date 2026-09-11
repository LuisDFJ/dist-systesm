package coordinator

import (
	"mr/shared"
)

func (c *Coordinator) Register( arg shared.ArgRegister, res *shared.ResRegister ) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	res.Id = shared.WorkerId(c.counter)
	res.N  = c.workers
	c.counter += 1
	return nil
}
