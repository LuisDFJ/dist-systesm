package worker

import (
	"bufio"
	"fmt"
	"strings"
	"mr/app/wc"
	"mr/shared"
	"mr/internal/types"
	"os"
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

	// KeyValue Collection
	col := types.Collection{}
	// Read Files in Current Directory
	files,err := os.ReadDir(".")
	if err != nil { return err }
	for _,file := range files {
		// Find Valid Files: mr-X-Y.txt
		if strings.HasPrefix(file.Name(), "mr") && strings.HasSuffix(file.Name(), ".txt") {
			fileBucket := strings.Split(strings.Replace(file.Name(),".txt", "", 1), "-")[2]
			// Match For Y == bucket
			if fileBucket == fmt.Sprintf("%v", bucket+1) {
				// Read File Content
				content,err := os.ReadFile( file.Name() )
				if err != nil { return err }
				// For each line in content
				for _,line := range strings.Split(string(content), "\n") {
					line := strings.Split(line,",")
					if len(line) != 2 { continue }
					// Append Key,Value to Collection
					kv := shared.KeyValue{Key:line[0],Value:line[1]}
					col = append(col, kv)
				}
			}
		}
	}

	// Suffle and Reduce
	filename := fmt.Sprintf("mr-out-%v.txt", w.id+1)
	file,err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	writer := bufio.NewWriter(file)
	for kvs := range col.Suffle() {
		line := fmt.Sprintf( "%v,%v\n", kvs.Key, wc.Reduce(kvs) )
		writer.WriteString(line)
	}
	writer.Flush()
	return nil
}
