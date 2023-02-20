package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nFile int
	mworkers map[int]int
	files []string
	nReduce int
	buckets [][]KeyValue
	uncompReduce []int
	nWorker int
	pnWorker int
	nMap int
	inProgressTask map[string]int
	inProgressReduce map[int]int
	rworkers map[int]int
	Dcount int
	mu sync.Mutex
	sorted bool
}


// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register(args *Args, reply *Reply) error {
	
	if c.DoneMap() {
		reply.MapD = true
	} else {
		reply.MapD = false
		c.mu.Lock()
		//if args.Pin == -1 {
			
			reply.Pin = c.nWorker
			c.mworkers[reply.Pin] = -1
			c.rworkers[reply.Pin] = -1
			//fmt.Println("Master: Worker " + strconv.Itoa(c.nWorker) + " Registered")
		//} else {
			//reply.Pin = args.Pin
		//}
		c.nWorker += 1
		//fmt.Println("Number of Workers:" + strconv.Itoa(c.nWorker))
		c.mu.Unlock()
	}
	

	return nil
}

func (c *Coordinator) MapComplete(args *CompArgs, reply *CompReply) error {
	c.mu.Lock()
	val, ok := c.inProgressTask[args.File]
	if ok && val == args.Pin {
		
		delete(c.inProgressTask, args.File)
		c.mworkers[args.Pin] = -1
		for _, kv := range args.Inter {
			c.buckets[ihash(kv.Key) % c.nReduce] = (append(c.buckets[ihash(kv.Key) % c.nReduce], kv))
		}
		c.nMap++
		//fmt.Println("------------MMM------------"+ strconv.Itoa(c.nMap))
	}
	c.mu.Unlock()
	reply.MapD = c.DoneMap()
	

	return nil
}

func (c *Coordinator) MapTask(args *TaskArgs, reply *TaskReply) error {
	
	if c.DoneMap() {
		reply.MapD = true
	} else {
		reply.MapD = false
		c.mu.Lock()
		if len(c.files) > 0 {
			
			reply.File = c.files[0]
			//fmt.Println("Master: Sended file " + reply.File + " to Worker " + strconv.Itoa(args.Pin))
			c.files = c.files[1:]
			c.inProgressTask[reply.File] = args.Pin
			c.mworkers[args.Pin] = 0
			
		} else {
			//fmt.Println("Master: No file to send to Worker " + strconv.Itoa(args.Pin))
			reply.File = ""
		}
		c.mu.Unlock()
	}
	

	return nil
}

func (c *Coordinator) ReduceTask(args *TaskArgs, reply *ReduceReply) error {
	c.mu.Lock()
	if len(c.uncompReduce) > 0 && c.rworkers[args.Pin] == -1{
		
		reply.TaskNum = c.uncompReduce[0]
		c.uncompReduce = c.uncompReduce[1:]
		reply.File = c.buckets[reply.TaskNum]
		c.inProgressReduce[args.Pin] = reply.TaskNum
		c.rworkers[args.Pin] = 0
		//fmt.Println("Master: Sending task " + strconv.Itoa(reply.TaskNum) + " to Worker " + strconv.Itoa(args.Pin))
	} else {
		reply.TaskNum = -1
		//fmt.Println("Master: No reduce file to send to Worker " + strconv.Itoa(args.Pin))
	}
	
	reply.ReduceD = c.Done()
	c.mu.Unlock()
	
	return nil
}

func (c *Coordinator) ReduceComplete(args *ReduceArgs, reply *CompReply) error {
	c.mu.Lock()
	val, ok := c.inProgressReduce[args.Pin]
	if ok && val == args.TaskN {
		
		delete(c.inProgressReduce, args.Pin)
		c.rworkers[args.Pin] = -1
		c.Dcount++
		//fmt.Println("Reduce Task " + strconv.Itoa(args.TaskN) + " Completed!!!")
		//fmt.Println("------------RRR------------"+ strconv.Itoa(c.Dcount))
	}
	
	reply.MapD = c.Done()
	c.mu.Unlock()
	
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = 10

	reply.Y = args.X + 10
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	return c.Dcount >= c.nReduce
}

func (c *Coordinator) DoneMap() bool {
	if c.nMap == c.nFile {
		if !c.sorted {
			c.mu.Lock()
			for _, i := range c.buckets {
				sort.Sort(ByKey(i))
			}
			c.mu.Unlock()
			c.sorted = true
		}	
		return true
	} else {
		return false
	}
}

func (c *Coordinator) rm(fls []string, fl string) []string {
	rmidx := 0
	for idx, f := range fls {
		if f == fl {
			rmidx = idx
			break
		}
	}
	fls = append(fls[:rmidx], fls[rmidx + 1:]...)
	return fls
}



func search(m map[string]int, w int) string {
	for i := range m {
		if m[i] == w {
			return i
		}
	}
	return ""
}

func (c *Coordinator) mwks() {
	for !c.DoneMap(){
		time.Sleep(time.Second)
		c.mu.Lock()
		for i := range c.mworkers {
			if c.mworkers[i] != -1 {
				c.mworkers[i]++
			}
			if c.mworkers[i] == 10 {
				fl := search(c.inProgressTask, i)
				c.files = append(c.files, fl)
				delete(c.inProgressTask, fl)
				c.mworkers[i] = -1
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) rwks() {
	for !c.Done(){
		time.Sleep(time.Second)
		c.mu.Lock()
		for i := range c.rworkers {
			if c.rworkers[i] != -1 {
				c.rworkers[i]++
			}
			if c.rworkers[i] == 10 {
				fl := c.inProgressReduce[i]
				//if ok {
					c.uncompReduce = append(c.uncompReduce, fl)
					delete(c.inProgressReduce, i)
					c.rworkers[i] = -1
				//}
			}
		}
		c.mu.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mworkers = map[int]int{}
	c.rworkers = map[int]int{}
	c.files = files
	c.nReduce = nReduce
	c.buckets = [][]KeyValue{}
	for i := 0; i < nReduce; i++ {
		c.buckets = append(c.buckets, []KeyValue{})
	}
	c.uncompReduce = []int{}
	for i := 0; i < nReduce; i++ {
		c.uncompReduce = append(c.uncompReduce, i)
	}
	c.nWorker = 0
	c.pnWorker = 0
	c.nMap = 0
	c.nFile = len(files)
	c.inProgressTask = map[string]int{}
	c.inProgressReduce = map[int]int{}
	c.Dcount = 0
	c.sorted = false

	go c.mwks()
	go c.rwks()

	c.server()
	return &c
}
