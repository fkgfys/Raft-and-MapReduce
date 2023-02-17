package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
type Args struct {
	Pin int
}

type Reply struct {
	Pin int
	MapD bool
	M map[int]int
}

type CompArgs struct {
	File string
	Pin int
	Inter []KeyValue
}

type CompReply struct {
	MapD bool
}

type TaskArgs struct {
	Pin int
}

type TaskReply struct {
	File string
	MapD bool
}

type ReduceReply struct {
	File []KeyValue
	ReduceD bool
	TaskNum int
}

type ReduceArgs struct {
	Pin int
	TaskN int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
