package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const waitTime = time.Second * 10

type TaskType int
type TaskStatus int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)
const (
	Idle TaskStatus = iota
	InProgress
	Finished
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int
	File     string
	WorkerId int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
}

func (c *Coordinator) GetReducePartitionCount(args *ReducePartitionCountArgs, reply *ReducePartitionCountReply) error {
	reply.Count = len(c.reduceTasks)
	return nil
}
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	var task *Task
	if c.nMap > 0 {
		task = selectTask(&c.mapTasks, args.WorkerId)
	} else if c.nReduce > 0 {
		task = selectTask(&c.reduceTasks, args.WorkerId)
	} else {
		task = &Task{
			Type:     ExitTask,
			Status:   Finished,
			Index:    -1,
			File:     "-1",
			WorkerId: -1,
		}
	}
	reply.TaskType = task.Type
	reply.TaskId = task.Index
	reply.File = task.File
	c.mu.Unlock()
	go c.waitForTask(task)
	return nil
}

func (c *Coordinator) PostTask(args *PostTaskCompleteArgs, reply *PostTaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MapTask {
		c.mapTasks[args.TaskId].Status = Finished
		c.mapTasks[args.TaskId].WorkerId = args.WorkerId
		c.nMap -= 1
	} else if args.TaskType == ReduceTask {
		c.reduceTasks[args.TaskId].Status = Finished
		c.reduceTasks[args.TaskId].WorkerId = args.WorkerId
		c.nReduce -= 1
	}
	reply.JobComplete = true
	return nil
}

func selectTask(tasks *[]Task, workerId int) *Task {
	var returnTask *Task
	size := len(*tasks)
	for i := 0; i < size; i++ {
		task := &(*tasks)[i]
		if task.Status == Idle {
			returnTask = task
			returnTask.Status = InProgress
			returnTask.WorkerId = workerId
			return returnTask
		}
	}
	return &Task{
		Type:     NoTask,
		Status:   Finished,
		Index:    -1,
		File:     "-1",
		WorkerId: -1,
	}
}

// Your code here -- RPC handlers for the worker to call.

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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.nMap == 0 && c.nReduce == 0
	return ret
}

func (c *Coordinator) waitForTask(task *Task) {
	if task.Type > 1 {
		return
	}
	<-time.After(waitTime)
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.Status == InProgress {
		task.Status = Idle
		task.WorkerId = -1
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]Task, c.nMap)
	c.reduceTasks = make([]Task, c.nReduce)
	c.mu = sync.Mutex{}

	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i] = Task{
			Type:     MapTask,
			Status:   Idle,
			Index:    i,
			File:     files[i],
			WorkerId: -1,
		}
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type:     ReduceTask,
			Status:   Idle,
			Index:    i,
			File:     "-1",
			WorkerId: -1,
		}
	}

	c.server()
	return &c
}
