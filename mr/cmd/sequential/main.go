package main

import (
	"fmt"
	"mr/app/wc"
	"mr/internal/types"
	"os"
)

func log( s string ) {
	fmt.Println("[SEQUENTIAL]: ", s)
}

func main() {
	if len(os.Args) < 2 {
		log("Wrong Usage - main.go inputfiles ...")
		os.Exit(1)
	}

	col := types.Collection{}
	for _,filename := range os.Args[1:] {
		content,err := os.ReadFile(filename)
		if err != nil {
			log("Unable to read file: " + filename)
			os.Exit(1)
		}
		kv := wc.Map(filename,string(content))
		col = append(col,kv...)
	}

	for kvs := range col.Suffle() {
		fmt.Println(kvs.Key, wc.Reduce(kvs))
	}
}

