package main

import (
	"fmt"
	"mr/shared"
	"net"
	"net/rpc"
	"net/http"
	"os"
	"time"
	"sync"
)

func log( s string ) {
	fmt.Println( "[Coordinator]: ", s )
}

type TaskStatus int
const (
	PENDING TaskStatus = iota
	RUNNING
	FINISHED
)


type MapTask struct {
	status 		TaskStatus
	workerId	shared.WorkerId
	file 			string
	timeLeft	time.Duration
}
type ReduceTask struct {
	status 		TaskStatus
	workerId	shared.WorkerId
	bucket 		shared.WorkerId
	timeLeft	time.Duration
}

type CoordinatorState int
const (
	MAP CoordinatorState = iota
	REDUCE
	EXIT
)

type Coordinator struct {
	mu sync.Mutex
	state CoordinatorState
	workers int
	mapTasks map[int]*MapTask
	reduceTasks map[int]*ReduceTask
	done bool
}

func New ( socket string, files []string, workers int ) *Coordinator {
	c := Coordinator{workers:workers}
	for id, file := range files {
		c.mapTasks[id] = &MapTask{file:file}
	}
	for id := range workers {
		bucket := shared.WorkerId(id)
		c.reduceTasks[id] = &ReduceTask{bucket:bucket}
	}
	c.server( socket )
	return &c
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

func (c *Coordinator) mapAssign(workerId shared.WorkerId) (bool,*shared.TaskParameters) {
	var params *shared.TaskParameters
	flag := true
	for id, task := range c.mapTasks {
		switch task.status {
			case PENDING:
				params = &shared.TaskParameters{
					Id: shared.TaskId(id),
					File: task.file,
				}
				flag = false
				c.changeMapTaskStatus(shared.TaskId(id), RUNNING, workerId)
				break
			case RUNNING: flag = false
			case FINISHED:
		}
	}
	return flag, params
}

func (c *Coordinator) reduceAssign(workerId shared.WorkerId) (bool,*shared.TaskParameters) {
	var params *shared.TaskParameters
	flag := true
	for id, task := range c.reduceTasks {
		switch task.status {
			case PENDING:
				params = &shared.TaskParameters{
					Id: shared.TaskId(id),
					Bucket: task.bucket,
				}
				flag = false
				c.changeReduceTaskStatus(shared.TaskId(id), RUNNING, workerId)
				break
			case RUNNING: flag = false
			case FINISHED:
		}
	}
	return flag, params
}

func (c *Coordinator) GetTask( req *shared.ArgGetTask, res *shared.ResGetTask ) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	retry := true
	for retry {
		retry = false
		switch c.state {
			case MAP:
				next, params := c.mapAssign(req.Id)
				if next {
					retry = true
					c.state = REDUCE
				} else if params != nil {
					res.State = shared.MAP
					res.Params = *params
				}
			case REDUCE:
				next, params := c.reduceAssign(req.Id)
				if next {
					retry = true
					c.state = EXIT
				} else if params != nil {
					res.State = shared.REDUCE
					res.Params = *params
				}
			case EXIT:
				res.State = shared.EXIT
		}
	}
	log( fmt.Sprintf("Assigning Work [type: %d, id: %d]", res.State, res.Params.Id ) )
	return nil
}

func (c *Coordinator) FinishTask( req *shared.ArgFinishTask, res *shared.ResFinishTask ) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch req.Type {
		case shared.MAP:
			c.changeMapTaskStatus(req.Id, FINISHED, shared.WorkerId(0))
		case shared.REDUCE:
			c.changeReduceTaskStatus(req.Id, FINISHED, shared.WorkerId(0))
	}

	log( fmt.Sprintf("Finishing Work [type: %d, id: %d]", req.Type, req.Id) )
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

