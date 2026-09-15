package worker

import (
	"bufio"
	"fmt"
	"mr/app/wc"
	"mr/shared"
	"os"
	"time"
)

func hash( s string, n int ) int {
	res := 0
	for i := range s {
		res += int(s[i])
	}
	return res % n
}

func (w *Worker) Map( filename string ) error {
	w.Log( fmt.Sprintf("Processing Map: %s", filename) )

	// Read File Content
	content,err := os.ReadFile(filename)
	if err != nil {
		w.Log("Unable to Read File")
		return err
	}

	// Map File Content to Key/Value Pairs
	res := wc.Map(filename,string(content))
	// Separate KeyValues in Buckets
	buckets := make([][]shared.KeyValue, w.n)
	for _,kv := range res {
		i := hash(kv.Key,w.n)
		buckets[i] = append(buckets[i],kv)
	}
	// Write Buckets to Intermediate Files
	for i := range buckets {
		name := fmt.Sprintf("temp-mr-%v-%v.txt", w.id+1, i+1)
		w.toCommit = append(w.toCommit,name)
		file,err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil { return err }
		defer file.Close()
		writer := bufio.NewWriter(file)
		for _,kv := range buckets[i] {
			_,err := writer.WriteString( fmt.Sprintf("%v,%v\n", kv.Key, kv.Value) )
			if err != nil { return err }
		}
		writer.Flush()
	}
	w.Log(fmt.Sprintf("%v", w.toCommit))
	return nil
}

func (w *Worker) Reduce( bucket int ) error {
	w.Log( fmt.Sprintf("Processing Reduce: %v", bucket) )

	time.Sleep(time.Second)
	return nil
}
