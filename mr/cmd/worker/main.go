package main

import (
	"time"
	"mr/internal/worker"
)

func main() {
	time.Sleep(time.Second)
	w,err := worker.New(":1234")
	if err != nil {
		w.Log(err.Error())
		return
	}
	w.Loop()
	w.Log("Exiting Worker")
}

