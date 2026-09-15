package worker

import (
	"fmt"
	"mr/shared"
)

func New( socket string ) (*Worker,error) {
	worker := Worker {
		socket:socket,
		toCommit:[]string{},
	}
	resp := shared.ResRegister{}
	err := worker.Register(shared.ArgRegister{}, &resp)
	if err != nil {
		worker.Log(err.Error())
	}
	worker.id = resp.Id
	worker.n  = resp.N
	return &worker,err
}

func (w *Worker) Log( s string ) {
	fmt.Printf("[WORKER %v/%v] : %s\n", w.id+1, w.n, s)
}

