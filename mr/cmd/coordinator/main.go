package main

import (
	"fmt"
	"mr/shared"
	"net"
	"net/rpc"
)

func log( s string ) {
	fmt.Println( "[Coordinator]: ", s )
}

type Coordinator struct {

}

func (c *Coordinator) GetWork( req shared.ReqGetWork, res *shared.ResGetWork ) error {
	log( "Assigning Work" )
	return nil
}

func (c *Coordinator) EndWork( req shared.ReqEndWork, res *shared.ResEndWork ) error {
	log( "Ending Work" )
	return nil
}

func main() {
	coordinator := new(Coordinator)
	rpc.Register(coordinator)
	rpc.HandleHTTP()

	l,err := net.Listen("tcp", ":1234")
	if err != nil {
		log("Fatal Error")
		return
	}
	defer l.Close()

	log("Coordinator Initialized")
	rpc.Accept(l)
}

