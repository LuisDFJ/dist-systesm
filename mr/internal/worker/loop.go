package worker

import (
	"os"
	"time"
	"bufio"
	"strings"
	"mr/shared"
)

func mergeFiles(old_file string, new_file string) error {
	if _,err := os.Stat(new_file); os.IsNotExist(err) {
		return os.Rename(old_file, new_file)
	}
	file,err := os.OpenFile(new_file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil { return err }
	defer file.Close()
	writer := bufio.NewWriter(file)
	content, err := os.ReadFile(old_file)
	if err != nil { return err }
	if _,err := writer.Write(content); err != nil { return err }
	writer.Flush()
	return os.Remove(old_file)
}

func (w *Worker) commit() error {
	for _,filename := range w.toCommit {
		w.Log("Commiting " + filename)
		new_filename := strings.Replace(filename, "temp-", "", 1 )
		err := mergeFiles(filename,new_filename)
		if err != nil { return err }
	}
	w.toCommit = []string{}
	return nil
}

func (w *Worker) cancel_commit() error {
	for _,filename := range w.toCommit {
		w.Log("Deleting " + filename)
		err := os.Remove(filename)
		if err != nil { return err }
	}
	w.toCommit = []string{}
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
	fail_attempts := 0
	MainLoop:
	for {
		time.Sleep(time.Second)
		task,err := w.getTask()
		if err != nil {
			w.Log(err.Error())
			fail_attempts += 1
			if fail_attempts > 3 { break MainLoop }
			continue MainLoop
		}
		fail_attempts = 0
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
		if err := w.finishTask(task); err != nil {
			w.Log( err.Error() )
			if err := w.cancel_commit(); err != nil {
				w.Log( err.Error() )
				break MainLoop
			}
			continue MainLoop
		}
		if err := w.commit(); err != nil {
			w.Log(err.Error())
			break MainLoop
		}
	}
}
