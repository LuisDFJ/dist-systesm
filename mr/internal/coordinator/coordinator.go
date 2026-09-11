package coordinator
import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

func (c *Coordinator) Log( s string ) {
	fmt.Println( "[Coordinator]: ", s )
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == EXIT
}

func New ( socket string, files []string, workers int ) *Coordinator {
	c := Coordinator{
		workers:workers,
		mapTasks:[]*MapTask{},
		reduceTasks:[]*ReduceTask{},
	}
	for id, file := range files {
		c.mapTasks[id] = &MapTask{file:file}
	}
	for id := range workers {
		c.reduceTasks[id] = &ReduceTask{bucket:id}
	}
	c.server( socket )
	return &c
}

func (c *Coordinator) server( socket string ) {
	// Initialize RPC Server with Coordinator
	rpc.Register(c)
	rpc.HandleHTTP()
	// Listen to TCP Socket
	l,err := net.Listen("tcp", socket)
	if err != nil {
		c.Log("Fatal Error")
		os.Exit(1)
	}
	c.Log("Initialized")
	go http.Serve(l, nil)
}
