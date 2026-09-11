package main

import (
	"time"
	"mr/internal/coordinator"
)

func main() {
	done := make(chan bool)
	c := coordinator.New( ":1234", []string{"file1.txt", "file2.txt"}, 10 )
	go c.Monitor( 500*time.Millisecond, done )
	for !c.Done() {
		time.Sleep(time.Second)
	}
	done <- true
	time.Sleep(time.Second)
	c.Log( "Job Done" )
}

