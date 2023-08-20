package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.824/mr"
	"log"
)
import "time"
import "os"
import "fmt"
import _ "net/http/pprof"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()

	//go func() {
	//	// Access as http://localhost:6060/debug/panicparse
	//	http.HandleFunc("/debug/panicparse", webstack.SnapshotHandler)
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()
	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	log.Println("Coordinator Exiting Job DONE")

	time.Sleep(time.Second)
}
