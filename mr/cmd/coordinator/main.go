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

type Coordinator struct {
	done bool
}

func (c *Coordinator) GetWork( req *shared.ReqGetWork, res *shared.ResGetWork ) error {
	log( "Assigning Work" )
	return nil
}

func (c *Coordinator) EndWork( req *shared.ReqEndWork, res *shared.ResEndWork ) error {
	log( "Ending Work" )
	return nil
}

func (c *Coordinator) Done() bool {
	return c.done
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

func New ( socket string, files []string ) *Coordinator {
	c := Coordinator{}
	log( fmt.Sprint(files) )
	c.server( socket )
	return &c
}

//TODO: Coordinator must initialize the RPC server.
//			Transfer initialization to a server() call
//			Arguments: files to map-reduce, socket to listen.

func main() {
	c := New( ":1234", []string{"file1.txt", "file2.txt"} )
	for !c.Done() {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
	log( "Job Done" )
}

