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
}
type ReduceTask struct {
	status 		TaskStatus
	workerId	shared.WorkerId
	bucket 		shared.WorkerId
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
				c.mapTasks[id].workerId = workerId
				c.mapTasks[id].status = RUNNING
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
				c.reduceTasks[id].workerId = workerId
				c.reduceTasks[id].status = RUNNING
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
			c.mapTasks[int(req.Id)].status = FINISHED
		case shared.REDUCE:
			c.reduceTasks[int(req.Id)].status = FINISHED
	}

	log( fmt.Sprintf("Finishing Work [type: %d, id: %d]", req.Type, req.Id) )
	return nil
}

func (c *Coordinator) Done() bool {
	return c.state == EXIT
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
	c := New( ":1234", []string{"file1.txt", "file2.txt"}, 10 )
	for !c.Done() {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
	log( "Job Done" )
}

