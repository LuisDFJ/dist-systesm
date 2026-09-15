package worker

import (
	"time"
)

func (w *Worker) Map( file string ) error {
	w.Log( "Working on Map ..." )

	time.Sleep(time.Second)
	return nil
}

func (w *Worker) Reduce( bucket int ) error {
	w.Log( "Working on Reduce ..." )
	time.Sleep(time.Second)
	return nil
}
