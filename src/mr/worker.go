package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args := WorkerArgs{}
		reply := CoordindatorArgs{}
		call("Coordinator.Implementation", &args, &reply)
		fmt.Println(reply)
		if reply.Task == "Done"{
			fmt.Println("Work Done")
			return
		}

		if reply.Task == "Map" {
			filename := reply.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			//create nReduce files for each Map task
			for i:=0 ; i< reply.Partitions ; i++ {
				fileCreatedName := fmt.Sprintf("mr-%v-%d", filename[:len(filename) - 4], i)
				file , _ := os.Create(fileCreatedName)
				file.Close()
			}

			for _, kv := range kva {
				// kv := kva[i]
				reduceTaskNumber := ihash(kv.Key)%reply.Partitions
				InermediateReduceFileName := fmt.Sprintf("mr-%v-%d", filename[:len(filename) - 4], reduceTaskNumber)
				IntermedateFile, err1 := os.OpenFile(InermediateReduceFileName, os.O_RDWR|os.O_APPEND, 0666)

				if err1 != nil {
					fmt.Println("intermediate file opening error")
					fmt.Println(err1.Error())
				}
				enc := json.NewEncoder(IntermedateFile)
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println(err.Error())
					log.Fatalf("cannot read or create IntermediateReduleFile %v", InermediateReduceFileName)
				}
				IntermedateFile.Close()
			}
		}
		if reply.Task == "Reduce" {
			// reduce all intermediate files for a reduce task
			kva := []KeyValue{}
			for _, IntermedateFileReduce := range reply.Files {
				temp := fmt.Sprintf("mr-%v-%d",IntermedateFileReduce[: len(IntermedateFileReduce) - 4],reply.ReduceTaskNumber)
				reduceFile, _ := os.Open(temp)
				dec := json.NewDecoder(reduceFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						fmt.Println(err)
				  		break 
					}
					kva = append(kva, kv)
				  }
				reduceFile.Close()
			}

			sort.Sort(ByKey(kva))
			oname := fmt.Sprintf("mr-out-%d", reply.ReduceTaskNumber)
			ofile, _ := os.Create(oname)

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
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

