package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

const LogEnabled = false

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ReducePartitionCountArgs struct {
}
type ReducePartitionCountReply struct {
	Count int
}

type GetTaskArgs struct {
	WorkerId int
}
type GetTaskReply struct {
	TaskType TaskType
	TaskId   int
	File     string
}
type PostTaskCompleteArgs struct {
	TaskType TaskType
	TaskId   int
	WorkerId int
}
type PostTaskCompleteReply struct {
	JobComplete bool
}

//type WorkerArgs struct {
//	Instruction          string
//	IntermediateFileName string
//}
//
//type CoordindatorArgs struct {
//	Task             string
//	FileName         string
//	ReduceTaskNumber int
//	Files            []string
//	Partitions       int
//}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func DPrintf(s string) {
	if LogEnabled == true {
		fmt.Println(s)
	}
}
