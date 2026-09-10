package main

import (
	"fmt"
	"mr/shared"
	"net"
	"net/rpc"
	"net/http"
	"os"
	"time"
)

func log( s string ) {
	fmt.Println( "[Coordinator]: ", s )
}


func New ( socket string, files []string, workers int ) *Coordinator {
	c := Coordinator{workers:workers,mapTasks:map[int]*MapTask{},reduceTasks:map[int]*ReduceTask{}}
	for id, file := range files {
		c.mapTasks[id] = &MapTask{file:file}
	}
	for id := range workers {
		c.reduceTasks[id] = &ReduceTask{bucket:id}
	}
	c.server( socket )
	return &c
}


func (c *Coordinator) FinishTask( arg shared.ArgFinishTask, res *shared.ResFinishTask ) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch arg.Type {
		case shared.MAP:
			c.changeMapTaskStatus(arg.Id, FINISHED, shared.WorkerId(0))
		case shared.REDUCE:
			c.changeReduceTaskStatus(arg.Id, FINISHED, shared.WorkerId(0))
	}

	log( fmt.Sprintf("Finishing Work [type: %d, id: %d]", arg.Type, arg.Id) )
	return nil
}

func (c *Coordinator) Register( arg shared.ArgRegister, res *shared.ResRegister ) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	res.Id = shared.WorkerId(c.counter)
	res.N  = c.workers
	c.counter += 1
	return nil
}

func (c *Coordinator) GetState() CoordinatorState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *Coordinator) Done() bool {
	return c.GetState() == EXIT
}

func (c *Coordinator) CheckReduceTimeLeft( interval time.Duration ) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dropTasks := []int{}
	for id, params := range c.reduceTasks {
		if params.status == RUNNING {
			params.timeLeft = max( params.timeLeft - interval, 0 )
			if params.timeLeft <= 0 { dropTasks = append(dropTasks, id) }
		}
	}
	for _, id := range dropTasks {
		c.changeReduceTaskStatus(shared.TaskId(id), PENDING, shared.WorkerId(0))
	}
}

func (c *Coordinator) CheckMapTimeLeft( interval time.Duration ) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dropTasks := []int{}
	for id, params := range c.mapTasks {
		if params.status == RUNNING {
			params.timeLeft = max( params.timeLeft - interval, 0 )
			if params.timeLeft <= 0 { dropTasks = append(dropTasks, id) }
		}
	}
	for _, id := range dropTasks {
		c.changeMapTaskStatus(shared.TaskId(id), PENDING, shared.WorkerId(0))
	}
}

func (c *Coordinator) monitor( interval time.Duration, done <-chan bool ) {
	ticker := time.NewTicker( interval )
	for {
		select {
			case <-done:
				ticker.Stop()
				return
			case <-ticker.C:
				switch c.GetState() {
					case MAP:
						c.CheckMapTimeLeft(interval)
					case REDUCE:
						c.CheckReduceTimeLeft(interval)
				}
		}
	}
}

func (c *Coordinator) server( socket string ) {
	rpc.Register(c)
	rpc.HandleHTTP()

	l,err := net.Listen("tcp", socket)
	if err != nil {
		log("Fatal Error")
		os.Exit(1)
	}
	log("Coordinator Initialized")
	go http.Serve(l, nil)
}


func main() {
	done := make(chan bool)
	c := New( ":1234", []string{"file1.txt", "file2.txt"}, 10 )
	go c.monitor(500*time.Millisecond, done)
	for !c.Done() {
		time.Sleep(time.Second)
	}
	done <- true
	time.Sleep(time.Second)
	log( "Job Done" )
}

