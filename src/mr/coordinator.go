package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	files     []string
	state     []string
	reduce    []string
	partition int
	ch        chan string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Implementation(args *WorkerArgs, reply *CoordindatorArgs) error {
	flag := 0
	fmt.Println(c.files)
	fmt.Println(c.state)
	fmt.Println(c.reduce)
	for i := 0; i < len(c.state); i++ {
		if c.state[i] == "idle" {
			c.state[i] = "in_progress"
			reply.Task = "Map"
			reply.FileName = c.files[i]
			reply.Partitions = c.partition
			flag = 1
			return nil
		}
	}
	for i := 0; i < len(c.reduce); i++ {
		if c.reduce[i] == "idle" {
			c.reduce[i] = "in_progress"
			reply.Files = make([]string, len(c.files))
			reply.Task = "Reduce"
			reply.ReduceTaskNumber = i
			reply.Files = c.files
			flag = 1
			return nil
		}
	}
	if flag == 0 {
		c.ch <- "I am done too"
		reply.Task = "Done"
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	select {
	case <-c.ch:
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.state = make([]string, len(files))
	c.reduce = make([]string, len(files))
	c.ch = make(chan string)
	c.partition = nReduce
	for i := 0; i < len(files); i++ {
		c.state[i] = "idle"
		c.reduce[i] = "idle"
	}

	c.server()
	return &c
}
