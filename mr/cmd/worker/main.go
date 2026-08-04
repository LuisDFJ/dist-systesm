package main

import (
	"fmt"
	"mr/shared"
	"net/rpc"
)

func log( s string ) {
	fmt.Println("[WORKER] :", s)
}


func call( socket string, f string, args any, resp any ) error {
	client, err := rpc.DialHTTP("tcp", "localhost" + socket)
	if err != nil {
		return fmt.Errorf("could not dial in coordinator: %v", err)
	}

	err = client.Call(f, args, resp)
	if err != nil {
		return fmt.Errorf("could not call: %s, %v", f, err)
	}
	return nil
}

func main() {

	args := shared.ReqGetWork{}
	resp := shared.ResGetWork{}

	err := call(":1234", "Coordinator.GetWork", &args, &resp)
	if err != nil {
		log(err.Error())
	}

	fmt.Printf( "%v \n", resp )
}

