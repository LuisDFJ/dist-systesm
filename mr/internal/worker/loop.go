package worker

import (
	"time"
	"mr/shared"
)

func (w *Worker) commit(task shared.ResGetTask) error {

	return nil
}

func (w *Worker) getTask() (shared.ResGetTask,error) {
	args := shared.ArgGetTask{}
	resp := shared.ResGetTask{}
	err := w.GetTask(args,&resp)
	return resp,err
}

func (w *Worker) finishTask( task shared.ResGetTask ) error {
		finishArgs := shared.ArgFinishTask{
			Id: task.Params.Id,
			Type: task.State,
		}
		return w.FinishTask( finishArgs, &shared.ResFinishTask{} )
}

func (w *Worker) Loop() {
	MainLoop:
	for {
		time.Sleep(time.Second)
		task,err := w.getTask()
		if err != nil {
			w.Log(err.Error())
			break MainLoop
		}
		switch task.State {
			case shared.MAP:
				if w.Map(task.Params.File) != nil { break MainLoop }
			case shared.REDUCE:
				if w.Reduce(task.Params.Bucket) != nil { break MainLoop }
			case shared.IDLE:
				continue MainLoop
			case shared.EXIT:
				break MainLoop
		}
		if w.finishTask(task) != nil {
			w.Log( err.Error() )
			continue MainLoop
		}
		if w.commit(task) != nil { break MainLoop }
	}
}
