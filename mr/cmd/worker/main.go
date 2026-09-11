package main

import (
	"fmt"
	"mr/shared"
	"net/rpc"
	"time"
)

type Worker struct {
	socket string
	id shared.WorkerId
	n  int
}

func New( socket string ) (*Worker,error) {
	worker := Worker {
		socket:socket,
	}
	resp := shared.ResRegister{}
	err := worker.Register(shared.ArgRegister{}, &resp)
	if err != nil {
		worker.log(err.Error())
	}
	worker.id = resp.Id
	worker.n  = resp.N
	return &worker,err
}

func (w *Worker) log( s string ) {
	fmt.Printf("[WORKER %v/%v] : %s\n", w.id+1, w.n, s)
}

func (w *Worker) Map( file string ) error {
	w.log( "Working on Map ..." )
	time.Sleep(time.Second)
	return nil
}

func (w *Worker) Reduce( bucket int ) error {
	w.log( "Working on Reduce ..." )
	time.Sleep(time.Second)
	return nil
}

func (w *Worker) Register( args shared.ArgRegister, resp *shared.ResRegister ) error {
	return call(w.socket, "Coordinator.Register", args, resp)
}

func (w *Worker) GetTask( args shared.ArgGetTask, resp *shared.ResGetTask ) error {
	return call(w.socket, "Coordinator.GetTask", args, resp)
}

func (w *Worker) FinishTask( args shared.ArgFinishTask, resp *shared.ResFinishTask ) error {
	return call(w.socket, "Coordinator.FinishTask", args, resp)
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
	time.Sleep(time.Second)
	w,err := New(":1234")
	if err != nil { return }
	Loop:
	for {
		time.Sleep(time.Second)
		args := shared.ArgGetTask{}
		resp := shared.ResGetTask{}
		err := w.GetTask(args,&resp)
		if err != nil {
			w.log(err.Error())
			break Loop
		}
		switch resp.State {
			case shared.MAP:
				if w.Map(resp.Params.File) != nil {
					break Loop
				}
				w.log( "Finished Map Task File: " + resp.Params.File )
			case shared.REDUCE:
				if w.Reduce(resp.Params.Bucket) != nil {
					break Loop
				}
				w.log( fmt.Sprintf("Finished Reduce Task Bucket: %v", resp.Params.Bucket) )
			case shared.IDLE:
				continue Loop
			case shared.EXIT:
				break Loop
		}
		finishArgs := shared.ArgFinishTask{
			Id: resp.Params.Id,
			Type: resp.State,
		}
		err = w.FinishTask( finishArgs, &shared.ResFinishTask{} )
		if err != nil {
			w.log( err.Error() )
		}
	}
	w.log("Exiting Worker")
}

