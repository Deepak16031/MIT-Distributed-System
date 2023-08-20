package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var nReduce int // number of reduce tasks to create

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	reducePartitionCountArgs := ReducePartitionCountArgs{}
	reducePartitionCountReply := ReducePartitionCountReply{}
	if call("Coordinator.GetReducePartitionCount", &reducePartitionCountArgs, &reducePartitionCountReply) == false {
		return
	}
	nReduce = reducePartitionCountReply.Count

	for {
		args := GetTaskArgs{os.Getppid()}
		reply := GetTaskReply{}
		call("Coordinator.GetTask", &args, &reply)
		DPrintf(fmt.Sprintf("Assigned task to the worker %v", reply))

		switch reply.TaskType {
		case MapTask:
			doMap(reply, mapf)
			postResult := rpcSuccess(reply)
			if postResult == false {
				DPrintf("Posting Job complete result fails")
				return
			}
		case ReduceTask:
			doReduce(reply, reducef)
			postResult := rpcSuccess(reply)
			if postResult == false {
				DPrintf("Posting Job complete result fails")
				return
			}
		case NoTask:
			//fmt.Println("No Task given, waiting for new task")
		case ExitTask:
			DPrintf("Job completed, exit!")
			return
		}
		//comment for running test script
		time.Sleep(time.Second)

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func rpcSuccess(reply GetTaskReply) bool {
	postSuccessArgs := PostTaskCompleteArgs{
		TaskType: reply.TaskType,
		TaskId:   reply.TaskId,
		WorkerId: os.Getppid(),
	}
	postSuccessReply := PostTaskCompleteReply{}
	result := call("Coordinator.PostTask", &postSuccessArgs, &postSuccessReply)
	return result && postSuccessReply.JobComplete
}

func doReduce(reply GetTaskReply, reducef func(string, []string) string) {
	// reduce all intermediate files for a reduce task
	fileNamePattern := fmt.Sprintf("mr-%v-%v", "*", reply.TaskId)
	IntermediateFiles, err1 := filepath.Glob(fileNamePattern)
	if err1 != nil {
		DPrintf(fmt.Sprintf("Error for finding files for the patter %v", fileNamePattern))
	}

	kva := getIntermediateFilesKeyValues(IntermediateFiles)
	writeReduceData(reply, reducef, kva)
}

func writeReduceData(reply GetTaskReply, reducef func(string, []string) string, kva []KeyValue) {
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
	ofile, err := os.Create(oname)

	if err != nil {
		DPrintf(fmt.Sprintf("Cannot create file : %v\n", oname))
	} else {
		DPrintf(fmt.Sprintf("Created file and written to it : %v", oname))

	}
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
}

func getIntermediateFilesKeyValues(IntermediateFiles []string) []KeyValue {
	var kva []KeyValue
	for _, IntermediateFileReduce := range IntermediateFiles {
		reduceFile, _ := os.Open(IntermediateFileReduce)
		dec := json.NewDecoder(reduceFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				//fmt.Println(err)
				break
			}
			kva = append(kva, kv)
		}
		reduceFile.Close()
	}
	return kva
}

func doMap(reply GetTaskReply, mapf func(string, string) []KeyValue) {
	filename := reply.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	defer file.Close()
	kva := mapf(filename, string(content))

	//create nReduce files for each Map task
	for i := 0; i < nReduce; i++ {
		fileCreatedName := fmt.Sprintf("mr-%v-%v-%d", os.Getpid(), reply.TaskId, i)
		file, _ := os.Create(fileCreatedName)
		file.Close()
	}
	for _, kv := range kva {
		// kv := kva[i]
		reduceTaskNumber := ihash(kv.Key) % nReduce
		IntermediateReduceFileName := fmt.Sprintf("mr-%v-%v-%d", os.Getpid(), reply.TaskId, reduceTaskNumber)
		IntermediateFile, err1 := os.OpenFile(IntermediateReduceFileName, os.O_RDWR|os.O_APPEND, 0666)

		if err1 != nil {
			fmt.Println("intermediate file opening error")
			fmt.Println(err1.Error())
		}
		enc := json.NewEncoder(IntermediateFile)
		err := enc.Encode(&kv)
		if err != nil {
			//fmt.Println(err.Error())
			log.Fatalf("cannot read or create IntermediateReduleFile %v", IntermediateReduceFileName)
		}
		IntermediateFile.Close()
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
