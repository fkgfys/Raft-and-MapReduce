package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// MAP PHASE
	wid := CallRegister()
	//fmt.Println("Worker " + strconv.Itoa(wid) + ": Registered")
	taskf, md := CallTask(wid)
	
	for !md {	// GOT A FILE
		filename := taskf

		if filename != "" {
			//fmt.Println("Got Task " + filename)
			intermediate := []KeyValue{}
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
			intermediate = append(intermediate, kva...)

			sort.Sort(ByKey(intermediate))

			md = CallCompMap(wid, intermediate, filename)
		}
		time.Sleep(300 * time.Millisecond)
		taskf, md = CallTask(wid)
	}

	// REDUCE PHASE
	reducet, num, d := CallRTask(wid)
	for !d {
		if num != -1 {
			oname := "mr-out-" + strconv.Itoa(num)
			//fmt.Println("Worker " + strconv.Itoa(wid) + ": Reducing Task " + strconv.Itoa(num))
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(reducet) {
				j := i + 1
				for j < len(reducet) && reducet[j].Key == reducet[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, reducet[k].Value)
				}
				output := reducef(reducet[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", reducet[i].Key, output)

				i = j
			}

			ofile.Close()
			CallCompReduce(wid, num)
		}
		time.Sleep(300 * time.Millisecond)
		reducet, num, d = CallRTask(wid)
	}
	


	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func contains(ls []int, s int) bool {
	for _, v := range ls {
		if v == s {
			return true
		}
	}
	return false
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

	args.X = 1000

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallRegister() int {
	args := Args{}
	args.Pin = -1
	reply := Reply{}
	call("Coordinator.Register", &args, &reply)
	return reply.Pin
}

func CallTask(p int) (string, bool) {
	args := TaskArgs{}
	reply := TaskReply{}
	args.Pin = p
	call("Coordinator.MapTask", &args, &reply)
	return reply.File, reply.MapD
}

func CallCompMap(p int, itr []KeyValue, fn string) bool {
	args := CompArgs{}
	reply := CompReply{}
	args.Inter = itr
	args.File = fn
	args.Pin = p
	call("Coordinator.MapComplete", &args, &reply)
	return reply.MapD
}

func CallRTask(p int) ([]KeyValue, int, bool) {
	args := TaskArgs{}
	reply := ReduceReply{}
	args.Pin = p
	call("Coordinator.ReduceTask", &args, &reply)
	return reply.File, reply.TaskNum, reply.ReduceD
}

func CallCompReduce(p int, fn int) bool {
	args := ReduceArgs{}
	reply := CompReply{}
	args.Pin = p
	args.TaskN = fn
	call("Coordinator.ReduceComplete", &args, &reply)
	return reply.MapD
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
