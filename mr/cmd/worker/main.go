package main

import (
	"fmt"
	"net/rpc"
	"mr/shared"
)

func log( s string ) {
	fmt.Println("[WORKER] :", s)
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log( "Could not dial in coordinator" )
		return
	}

	var res shared.ResGetWork
	err = client.Call( "Coordinator.GetWork", shared.ReqGetWork{}, &res )
	if err != nil {
		log( "Could not call GetWork" )
		return
	}

	fmt.Printf( "%v \n", res )
}

