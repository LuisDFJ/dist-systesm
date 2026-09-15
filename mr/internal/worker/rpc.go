package worker

import (
	"fmt"
	"net/rpc"
	"mr/shared"
)

// RPC Inteface Register Worker
func (w *Worker) Register( args shared.ArgRegister, resp *shared.ResRegister ) error {
	return call(w.socket, "Coordinator.Register", args, resp)
}

// RPC Interface Get Task
func (w *Worker) GetTask( args shared.ArgGetTask, resp *shared.ResGetTask ) error {
	return call(w.socket, "Coordinator.GetTask", args, resp)
}

// RPC Interface Finish Task
func (w *Worker) FinishTask( args shared.ArgFinishTask, resp *shared.ResFinishTask ) error {
	return call(w.socket, "Coordinator.FinishTask", args, resp)
}

func call( socket string, f string, args any, resp any ) error {
	// TCP Dial In to RPC Server
	client, err := rpc.DialHTTP("tcp", "localhost" + socket)
	// Connection Error
	if err != nil {
		return fmt.Errorf("could not dial in coordinator: %v", err)
	}
	err = client.Call(f, args, resp)
	// API Call Error
	if err != nil {
		return fmt.Errorf("could not call: %s, %v", f, err)
	}
	return nil
}
